"""Thread-safe, per-application sliding-window limits shared by auth and reviews."""

import threading
import time
from collections import OrderedDict, deque

from app.errors import AppError


class RateLimiter:
    def __init__(self) -> None:
        self.entries: OrderedDict[str, deque[float]] = OrderedDict()
        self.lock = threading.Lock()

    def check(self, key: str, limit: int, window: float = 60) -> None:
        now = time.monotonic()
        # Sync HTTP handlers share this limiter; admission and recording must be atomic.
        with self.lock:
            events = self.entries.setdefault(key, deque())
            while events and events[0] <= now - window:
                events.popleft()
            if len(events) >= limit:
                raise AppError("rate_limited", "Too many requests. Try again in one minute.", 429)
            events.append(now)
            self.entries.move_to_end(key)
            while len(self.entries) > 10000:
                self.entries.popitem(last=False)
