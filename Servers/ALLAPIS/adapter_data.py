from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, TypeVar


_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_DATA_ROOT = Path(__file__).resolve().parent / "DATA"
T = TypeVar("T")


class AdapterDataError(RuntimeError):
    pass


def load_version_json(
    api_version: str,
    filename: str,
    expected_type: type[T],
) -> T:
    """Load required, shipped adapter data without accepting arbitrary paths."""

    if not _SAFE_COMPONENT.fullmatch(api_version):
        raise AdapterDataError(f"Invalid adapter data version: {api_version!r}.")
    if (
        not _SAFE_COMPONENT.fullmatch(filename)
        or Path(filename).name != filename
        or Path(filename).suffix.casefold() != ".json"
    ):
        raise AdapterDataError(f"Invalid adapter data filename: {filename!r}.")
    path = (_DATA_ROOT / api_version / filename).resolve()
    expected_parent = (_DATA_ROOT / api_version).resolve()
    if path.parent != expected_parent:
        raise AdapterDataError("Adapter data path escaped its version directory.")
    try:
        value: Any = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise AdapterDataError(
            f"Required adapter data file is missing: DATA/{api_version}/{filename}."
        ) from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AdapterDataError(
            f"Adapter data file is invalid: DATA/{api_version}/{filename} "
            f"({type(exc).__name__})."
        ) from exc
    if not isinstance(value, expected_type):
        raise AdapterDataError(
            f"DATA/{api_version}/{filename} must contain a JSON "
            f"{expected_type.__name__}."
        )
    return value


def int_keyed_dict(value: dict[str, Any], *, filename: str) -> dict[int, Any]:
    result: dict[int, Any] = {}
    for key, item in value.items():
        try:
            result[int(key)] = item
        except (TypeError, ValueError) as exc:
            raise AdapterDataError(
                f"{filename} contains a non-integer mapping key: {key!r}."
            ) from exc
    return result
