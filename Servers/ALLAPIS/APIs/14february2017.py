"""14 February 2017 Rec Room HTTP/WebSocket API adapter.

Confirmed from decompiled client build 7452447116407047837:
- RecNet subscription sync still calls api/PlayerSubscriptions/v1/init,
  api/PlayerSubscriptions/v1/add, and api/PlayerSubscriptions/v1/remove with a
  raw JSON array of player ids.
- The client also calls the v2/v3 account route family already handled by the
  shared adapter chain: api/avatar/v2, api/avatar/v2/set,
  api/avatar/v3/items, api/avatar/v2/gifts, api/settings/v2,
  api/presence/v2, api/messages/v2, api/images/v2/profile,
  api/players/v2/objective, and api/relationships/v2.
- Local-player endpoints accept X-Rec-Room-Profile when present and otherwise
  fall back to the newest stored legacy profile for this client family.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

API_VERSION = "14february2017"
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = "Tue, 14 Feb 2017 02:05:12 GMT"


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
    module_path = Path(__file__).with_name("13february2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_13february2017_shared_for_14february2017", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 13february2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()

handle_http = _SHARED.handle_http
handle_websocket = _SHARED.handle_websocket
