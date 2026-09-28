import asyncio
import random
import time
from collections import Counter, deque
from datetime import timezone
from email.utils import parsedate_to_datetime

import aiohttp

TARGET_URL = "https://example.com"

MAX_WORKERS = 15
TOTAL_REQUESTS = 100
MAX_ATTEMPTS = 3
QUEUE_JOIN_TIMEOUT = 300

PROXY_FAILURE_LIMIT = 3
PROXY_SUCCESS_RESET = 2

BACKOFF_BASE = 0.5
BACKOFF_JITTER = 0.25
BACKOFF_CAP = 8.0

DEFAULT_COOLDOWN = 5.0
MAX_COOLDOWN = 60.0

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10, connect=5, sock_read=8)

BAD_PROXY_STATUSES = frozenset({403, 407, 502, 503, 504})
NETWORK_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, OSError)

PROXY_LIST = [
    "http://192.168.1.10:8080",
    "http://192.168.1.11:3128",
    "http://10.0.0.5:8888",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]


class ProxyRejected(Exception):
    pass


class TargetRateLimited(Exception):
    def __init__(self, retry_after: float) -> None:
        super().__init__(f"retry_after={retry_after:.1f}s")
        self.retry_after = retry_after


class Cooldown:
    def __init__(self) -> None:
        self._until = 0.0

    def set(self, seconds: float) -> None:
        self._until = max(self._until, time.monotonic() + seconds)

    async def wait(self) -> None:
        while True:
            delay = self._until - time.monotonic()
            if delay <= 0:
                return
            await asyncio.sleep(delay + random.uniform(0, 0.25))


class Stats:
    def __init__(self) -> None:
        self.ok = 0
        self.failed = 0
        self.bytes = 0
        self.status = Counter()
        self.errors = Counter()


class ProxyPool:
    def __init__(self, proxies, failure_limit: int, success_reset: int) -> None:
        self._proxies = list(proxies)
        self._failures = dict.fromkeys(self._proxies, 0)
        self._streaks = dict.fromkeys(self._proxies, 0)
        self._failure_limit = failure_limit
        self._success_reset = success_reset
        self._bag = deque()
        self._dead_logged = False

    @property
    def proxies(self):
        return tuple(self._proxies)

    def failure_count(self, proxy: str) -> int:
        return self._failures[proxy]

    def _refill(self) -> None:
        alive = [p for p in self._proxies if self._failures[p] < self._failure_limit]
        if alive:
            self._dead_logged = False
        else:
            if not self._dead_logged:
                print("[pool] every proxy quarantined -> health reset")
                self._dead_logged = True
            self._failures = dict.fromkeys(self._proxies, 0)
            self._streaks = dict.fromkeys(self._proxies, 0)
            alive = list(self._proxies)
        self._bag.extend(random.sample(alive, len(alive)))

    def acquire(self, avoid: str | None = None) -> str:
        for _ in range(2):
            if not self._bag:
                self._refill()
            candidates = [p for p in self._bag if self._failures[p] < self._failure_limit]
            if not candidates:
                self._bag.clear()
                self._refill()
                continue
            fresh = [p for p in candidates if p != avoid]
            proxy = random.choice(fresh) if fresh else random.choice(candidates)
            self._bag.remove(proxy)
            return proxy
        self._refill()
        alive = [p for p in self._proxies if self._failures[p] < self._failure_limit]
        pool = alive or list(self._proxies)
        proxy = random.choice(pool)
        self._bag = deque(p for p in pool if p != proxy)
        return proxy

    def report_success(self, proxy: str) -> None:
        self._streaks[proxy] += 1
        if self._streaks[proxy] >= self._success_reset:
            self._failures[proxy] = 0
            self._streaks[proxy] = 0
        else:
            self._failures[proxy] = max(0, self._failures[proxy] - 1)

    def report_failure(self, proxy: str) -> None:
        self._failures[proxy] += 1
        self._streaks[proxy] = 0
        if self._failures[proxy] >= self._failure_limit:
            print(f"[pool] quarantined: {proxy} ({self._failures[proxy]} failures)")


def backoff_delay(attempt: int) -> float:
    return min(BACKOFF_CAP, BACKOFF_BASE * 2 ** (attempt - 1)) + random.uniform(0, BACKOFF_JITTER)


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, when.timestamp() - time.time())


async def fetch(session, pool: ProxyPool, cooldown: Cooldown, stats: Stats, index: int) -> None:
    previous: str | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        await cooldown.wait()
        proxy = pool.acquire(avoid=previous)
        previous = proxy
        headers = {"User-Agent": random.choice(USER_AGENTS)}

        try:
            async with session.get(TARGET_URL, proxy=proxy, headers=headers) as response:
                body = await response.read()
                if response.status in BAD_PROXY_STATUSES:
                    raise ProxyRejected(f"status={response.status}")
                if response.status == 429:
                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    raise TargetRateLimited(
                        DEFAULT_COOLDOWN if retry_after is None else retry_after
                    )
                pool.report_success(proxy)
                stats.ok += 1
                stats.bytes += len(body)
                stats.status[response.status] += 1
                print(
                    f"[{index}] {response.status} bytes={len(body)} "
                    f"proxy={proxy} attempt={attempt}"
                )
                return

        except TargetRateLimited as exc:
            cooldown.set(min(MAX_COOLDOWN, exc.retry_after))
            stats.errors["rate_limited"] += 1
            print(f"[{index}] attempt {attempt}/{MAX_ATTEMPTS} 429 {exc}")

        except ProxyRejected as exc:
            pool.report_failure(proxy)
            stats.errors["proxy_rejected"] += 1
            print(
                f"[{index}] attempt {attempt}/{MAX_ATTEMPTS} "
                f"{exc} proxy={proxy}"
            )

        except NETWORK_ERRORS as exc:
            pool.report_failure(proxy)
            stats.errors[type(exc).__name__] += 1
            print(
                f"[{index}] attempt {attempt}/{MAX_ATTEMPTS} "
                f"{type(exc).__name__}: {exc} proxy={proxy}"
            )

        if attempt < MAX_ATTEMPTS:
            await asyncio.sleep(backoff_delay(attempt))

    stats.failed += 1
    print(f"[{index}] gave up after {MAX_ATTEMPTS} attempts")


async def worker(queue, session, pool: ProxyPool, cooldown: Cooldown, stats: Stats, worker_id: int) -> None:
    while True:
        index = await queue.get()
        try:
            if index is None:
                return
            try:
                await fetch(session, pool, cooldown, stats, index)
            except Exception as exc:
                stats.errors[f"worker:{type(exc).__name__}"] += 1
                print(f"[worker {worker_id}] unexpected {type(exc).__name__}: {exc}")
        finally:
            queue.task_done()


def report(pool: ProxyPool, stats: Stats) -> None:
    total = stats.ok + stats.failed
    print("---- summary ----")
    print(f"ok={stats.ok} failed={stats.failed} total={total} bytes={stats.bytes}")
    print(f"status: {dict(stats.status)}")
    print(f"errors: {dict(stats.errors)}")
    print(f"proxies: { {p: pool.failure_count(p) for p in pool.proxies} }")


async def main() -> None:
    queue: asyncio.Queue = asyncio.Queue()
    for i in range(1, TOTAL_REQUESTS + 1):
        queue.put_nowait(i)

    pool = ProxyPool(PROXY_LIST, PROXY_FAILURE_LIMIT, PROXY_SUCCESS_RESET)
    cooldown = Cooldown()
    stats = Stats()
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS, limit_per_host=MAX_WORKERS)

    async with aiohttp.ClientSession(connector=connector, timeout=REQUEST_TIMEOUT) as session:
        workers = [
            asyncio.create_task(worker(queue, session, pool, cooldown, stats, wid))
            for wid in range(MAX_WORKERS)
        ]
        try:
            await asyncio.wait_for(queue.join(), QUEUE_JOIN_TIMEOUT)
        except asyncio.TimeoutError:
            print("[main] queue.join() timed out -> cancelling workers")
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        else:
            for _ in workers:
                queue.put_nowait(None)
            await asyncio.gather(*workers, return_exceptions=True)

    report(pool, stats)


if __name__ == "__main__":
    asyncio.run(main())