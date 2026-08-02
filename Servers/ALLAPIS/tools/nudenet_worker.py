from __future__ import annotations

import json
import sys
from typing import Any


_detector: Any | None = None


def detector() -> Any:
    global _detector
    if _detector is None:
        from nudenet import NudeDetector

        _detector = NudeDetector()
    return _detector


def respond(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main() -> None:
    for line in sys.stdin:
        request: dict[str, Any] | None = None
        try:
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise ValueError("Request must be a JSON object.")
            request = parsed
            detections = detector().detect(str(request.get("path") or ""))
            if not isinstance(detections, list):
                raise RuntimeError("NudeNet returned an invalid detection payload.")
            respond(
                {
                    "id": request.get("id"),
                    "ok": True,
                    "detections": [
                        item for item in detections if isinstance(item, dict)
                    ],
                }
            )
        except Exception as exc:
            respond(
                {
                    "id": request.get("id") if isinstance(request, dict) else None,
                    "ok": False,
                    "error": str(exc),
                }
            )


if __name__ == "__main__":
    main()
