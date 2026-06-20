"""3 February 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 4682896605443297458:
- RecNet files are byte-identical to the 1 February 2017 client family.
- Profile fields remain CodeStage obscured client-side types, with unchanged
  JSON keys and endpoint payloads.
- Push notifications use api/notification/v2.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "3february2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 03 Feb 2017 23:53:02 GMT"


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
    module_path = Path(__file__).with_name("1february2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_1february2017_shared_for_3february2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 1february2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http
handle_websocket = _SHARED.handle_websocket
