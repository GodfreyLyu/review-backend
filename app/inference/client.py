"""Review client boundary; implementations never own a model inside the backend."""

import threading
from typing import Protocol


class ReviewClient(Protocol):
    def connect(self) -> None:
        """Try one handshake; raise InferenceUnavailable for a transient dependency failure."""
        ...

    def review(self, source: str, language: str, stop: threading.Event) -> str: ...
    def close(self) -> None: ...
