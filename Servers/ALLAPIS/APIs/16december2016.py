"""16 December 2016 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 8356502470941083689:
- The HTTP/WebSocket route surface matches the 9 December 2016 client family.
- Startup probes api/versioncheck/v1, downloads api/config/v2, then calls
  api/players/v1/getorcreate.
- Local-player routes use X-Rec-Room-Profile and the v2/v3 endpoint family.
- Push notifications use api/notification/v2.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "16december2016"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Fri, 16 Dec 2016 19:35:04 GMT"


def _load_shared_adapter():
    module_path = Path(__file__).with_name("9december2016.py")
    spec = importlib.util.spec_from_file_location("recroom_api_9december2016_shared_for_16december2016", module_path)
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
