"""23 February 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 6859627191769621156:
- The HTTP/WebSocket route surface matches the 22 February 2017 client family.
- Objective completion uses POST api/players/v1/objectives.
- Player subscription synchronization remains notification-WebSocket driven
  rather than the older REST api/PlayerSubscriptions/v1/init/add/remove surface.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "23february2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Thu, 23 Feb 2017 23:40:18 GMT"


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
    module_path = Path(__file__).with_name("22february2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_22february2017_shared_for_23february2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 22february2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http
handle_websocket = _SHARED.handle_websocket
