"""7 January 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 1355637356417786081:
- The HTTP/WebSocket route surface still matches the late-December 2016 family.
- Startup probes api/versioncheck/v1, creates/loads the local profile through
  api/players/v1/getorcreate, and downloads api/config/v2.
- Local-player routes use X-Rec-Room-Profile and the v2/v3 endpoint family.
- Game session lookups use api/gamesessions/v1/ and api/gamesessions/v1/<Id>.
- Push notifications use api/notification/v2.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "7january2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Sat, 07 Jan 2017 02:41:28 GMT"


def _load_shared_adapter():
    module_path = Path(__file__).with_name("9december2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_9december2016_shared_for_7january2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 9december2016 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.API_VERSION = API_VERSION
    module.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._BASE.API_VERSION = API_VERSION
    module._BASE.DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = DEFAULT_PROFILE_IMAGE_LAST_MODIFIED
    module._BASE._set_api_version(module._BASE)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http
handle_websocket = _SHARED.handle_websocket
