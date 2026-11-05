"""Subprocess crash barriers, disabled unless ENVIRONMENT=test."""

import os
import time
from pathlib import Path


def pause(settings, variable, stop=None):
    value = os.environ.get(variable)
    if settings.environment != "test" or not value:
        return
    path = Path(value)
    path.touch()
    while path.exists() and not (stop is not None and stop.is_set()):
        time.sleep(0.05)
