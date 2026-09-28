import asyncio
import json
import random
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

FEEDS = [
    "https://pythonbytes.fm/rss",
    "https://talkpython.fm/rss",
    "https://changelog.com/podcast/feed",
    "https://lexfridman.com/feed/podcast/",
]

MAX_WORKERS = 5
MAX_ATTEMPTS = 3
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=10, sock_read=15)

BACKOFF_BASE = 0.5
BACKOFF_JITTER = 0.25
BACKOFF_CAP = 8.0
QUEUE_JOIN_TIMEOUT = 180

FETCH_TRANSCRIPTS = True
TRANSCRIPT_LIMIT = 10

USER_AGENT = "aiohttp-metadata-collector/1.0 (research pet project; +github.com/aroslavsuevalov-blip/aiohttp-proxy-getter)"

OUT_DIR = Path("out")
METADATA_FILE = OUT_DIR / "metadata.jsonl"
TRANSCRIPTS_DIR = OUT_DIR / "transcripts"

NETWORK_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, OSError)

NS = {
    "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
    "podcast": "https://podcastindex.org/namespace/1.0",
    "atom": "http://www.w3.org/2005/Atom",
    "content": "http://purl.org/rss/1.0/modules/content/",
}


def backoff_delay(attempt: int) -> float:
    return min(BACKOFF_CAP, BACKOFF_BASE * 2 ** (attempt - 1)) + random.uniform(0, BACKOFF_JITTER)


async def fetch_bytes(session: aiohttp.ClientSession, url: str) -> bytes | None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.read()
                print(f"[fetch] {response.status} {url}")
                if 400 <= response.status < 500 and response.status != 429:
                    return None
        except NETWORK_ERRORS as exc:
            print(f"[fetch] attempt {attempt}/{MAX_ATTEMPTS} {type(exc).__name__} {url}")
        if attempt < MAX_ATTEMPTS:
            await asyncio.sleep(backoff_delay(attempt))
    return None


def text(node: ET.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    value = node.text.strip()
    return value or None


def parse_feed(data: bytes, feed_url: str) -> list[dict]:
    root = ET.fromstring(data)
    items = []
    for item in root.iter("item"):
        transcript = item.find("podcast:transcript", NS)
        enclosure = item.find("enclosure")
        items.append(
            {
                "feed": feed_url,
                "title": text(item.find("title")),
                "link": text(item.find("link")),
                "guid": text(item.find("guid")),
                "pub_date": text(item.find("pubDate")),
                "duration": text(item.find("itunes:duration", NS)),
                "author": text(item.find("itunes:author", NS)),
                "episode": text(item.find("itunes:episode", NS)),
                "audio_url": enclosure.get("url") if enclosure is not None else None,
                "audio_type": enclosure.get("type") if enclosure is not None else None,
                "transcript_url": transcript.get("url") if transcript is not None else None,
                "transcript_type": transcript.get("type") if transcript is not None else None,
            }
        )
    for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
        link = entry.find("atom:link[@rel='alternate']", NS)
        if link is None:
            link = entry.find("atom:link", NS)
        items.append(
            {
                "feed": feed_url,
                "title": text(entry.find("atom:title", NS)),
                "link": link.get("href") if link is not None else None,
                "guid": text(entry.find("atom:id", NS)),
                "pub_date": text(entry.find("atom:published", NS))
                or text(entry.find("atom:updated", NS)),
                "transcript_url": None,
                "transcript_type": None,
            }
        )
    return items


async def collect_feed(feed_url: str, session, semaphore, items_out: list, stats: dict) -> None:
    async with semaphore:
        data = await fetch_bytes(session, feed_url)
    if data is None:
        stats["feeds_failed"] += 1
        return
    try:
        items = parse_feed(data, feed_url)
    except ET.ParseError as exc:
        print(f"[parse] {feed_url}: {exc}")
        stats["feeds_failed"] += 1
        return
    stats["feeds_ok"] += 1
    stats["items"] += len(items)
    items_out.extend(items)
    print(f"[feed] {len(items):4d} items {feed_url}")


async def collect_transcripts(items: list[dict], session, semaphore, stats: dict) -> None:
    targets = [i for i in items if i.get("transcript_url")][:TRANSCRIPT_LIMIT]
    seen: set[str] = set()

    async def one(item: dict) -> None:
        url = item["transcript_url"]
        if url in seen:
            return
        seen.add(url)
        async with semaphore:
            data = await fetch_bytes(session, url)
        if data is None:
            stats["transcripts_failed"] += 1
            return
        slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", urlparse(url).path.rsplit("/", 1)[-1]) or "transcript"
        path = TRANSCRIPTS_DIR / slug
        path.write_bytes(data)
        stats["transcripts_ok"] += 1
        print(f"[transcript] {len(data):7d} B {path}")

    await asyncio.gather(*(one(i) for i in targets))


async def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

    stats = {"feeds_ok": 0, "feeds_failed": 0, "items": 0, "transcripts_ok": 0, "transcripts_failed": 0}
    headers = {"User-Agent": USER_AGENT}
    semaphore = asyncio.Semaphore(MAX_WORKERS)

    async with aiohttp.ClientSession(headers=headers, timeout=REQUEST_TIMEOUT) as session:
        items: list[dict] = []
        tasks = [asyncio.create_task(collect_feed(f, session, semaphore, items, stats)) for f in FEEDS]
        done, pending = await asyncio.wait(tasks, timeout=QUEUE_JOIN_TIMEOUT)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        with METADATA_FILE.open("w", encoding="utf-8") as fh:
            for item in items:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")

        if FETCH_TRANSCRIPTS and items:
            await collect_transcripts(items, session, semaphore, stats)

    print("---- summary ----")
    print(f"feeds: ok={stats['feeds_ok']} failed={stats['feeds_failed']}")
    print(f"items: {stats['items']} -> {METADATA_FILE}")
    print(f"transcripts: ok={stats['transcripts_ok']} failed={stats['transcripts_failed']} -> {TRANSCRIPTS_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
