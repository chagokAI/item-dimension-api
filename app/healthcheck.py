"""Worker readiness: the polling loop must have a Kafka partition assignment."""
import json
import os
import time
from pathlib import Path

HEALTH_FILE = Path(os.getenv("DL_HEALTH_FILE", "/tmp/chagok-worker-health.json"))


def heartbeat(ready: bool) -> None:
    temporary = HEALTH_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps({"ready": ready, "updated": time.monotonic()}))
    temporary.replace(HEALTH_FILE)


def healthy() -> bool:
    try:
        status = json.loads(HEALTH_FILE.read_text())
        age = time.monotonic() - status["updated"]
        return status["ready"] is True and 0 <= age < float(os.getenv("DL_HEALTH_TIMEOUT", "120"))
    except (OSError, ValueError, KeyError, TypeError):
        return False


if __name__ == "__main__":
    raise SystemExit(0 if healthy() else 1)
