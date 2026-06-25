"""17 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from the game build at manifest 1867696127010072960:
- RecNet still uses COGCNMJCNKN.
- HTTP/WebSocket URL fields remain EHBCBOGDLDB and FPGKGDJLOJJ.
- Login posts the same real /api/platformlogin/v1 form as 13 March.
- Player subscription synchronization remains notification-WebSocket driven;
  REST api/PlayerSubscriptions/v1/init/add/remove is not a real route here.
- Messages include v2 get/send/delete, v1 sendMultiple, and offline invites.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "17march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 17 Mar 2017 19:39:02 GMT"


def _retarget_module(module) -> None:
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    if hasattr(module, "_BASE"):
        module._BASE.API_VERSION = API_VERSION
        module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
        module._BASE._set_api_version(module._BASE)
    if hasattr(module, "_PLATFORM_BASE"):
        module._PLATFORM_BASE.API_VERSION = API_VERSION
    if hasattr(module, "_SHARED"):
        _retarget_module(module._SHARED)


def _load_shared_adapter():
    module_path = Path(__file__).with_name("13march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_13march2017_shared_for_17march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 13march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http
handle_websocket = _SHARED.handle_websocket
