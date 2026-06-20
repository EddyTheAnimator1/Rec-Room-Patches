"""26 January 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 151686977716385839:
- RecNet route strings match the 20 January 2017 client family.
- Player reputation healing uses api/playerReputation/v1/heal.
- Player reporting uses api/PlayerReporting/v1/create with the same payload.
- Push notifications use api/notification/v2.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "26january2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Thu, 26 Jan 2017 01:14:27 GMT"


def _retarget_module(module) -> None:
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    if hasattr(module, "_BASE"):
        module._BASE.API_VERSION = API_VERSION
        module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
        module._BASE._set_api_version(module._BASE)
    if hasattr(module, "_SHARED"):
        _retarget_module(module._SHARED)


def _load_shared_adapter():
    module_path = Path(__file__).with_name("20january2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_20january2017_shared_for_26january2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 20january2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http
handle_websocket = _SHARED.handle_websocket
