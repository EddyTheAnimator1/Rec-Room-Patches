"""1 March 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from the game build at manifest 6135625719507137531:
- The client still uses POST api/players/v1/getorcreate for login/bootstrap.
- There is no real api/platformlogin/v1 route in this build.
- Server config is GET api/config/v2.
- Objective completion uses POST api/players/v1/objectives.
- Player subscription synchronization remains notification-WebSocket driven
  rather than the older REST api/PlayerSubscriptions/v1/init/add/remove surface.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "1march2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Wed, 01 Mar 2017 21:37:31 GMT"


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
    module_path = Path(__file__).with_name("24february2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_24february2017_shared_for_1march2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 24february2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http
handle_websocket = _SHARED.handle_websocket
