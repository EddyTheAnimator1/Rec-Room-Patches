from __future__ import annotations

import asyncio
import importlib.util
import hashlib
import hmac
import json
import re
import secrets
import shutil
import subprocess
import threading
import urllib.parse
from collections import Counter
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse, Response
from starlette.websockets import WebSocketDisconnect

from content_filter import ContentFilterError, ProhibitedProfileText
from adapter_data import int_keyed_dict, load_version_json

API_VERSION = "25april2019"
_SERVICE_CONFIG = load_version_json(API_VERSION, "service_config.json", dict)
DEFAULT_PROFILE_IMAGE_LAST_MODIFIED = str(_SERVICE_CONFIG["default_profile_image_last_modified"])
TOKEN_BALANCE = int(_SERVICE_CONFIG["default_balances"]["tokens"])
CHEER_BALANCE = int(_SERVICE_CONFIG["default_balances"]["cheers"])
PURCHASE_GIFT_MESSAGE = str(_SERVICE_CONFIG["purchase_gift_message"])
DEFAULT_PLAYER_BIO = str(_SERVICE_CONFIG["default_player_bio"])
COACH_PLAYER_UUID = str(_SERVICE_CONFIG["coach_player_uuid"])
CANONICAL_ACCOUNT_EMAIL = str(_SERVICE_CONFIG["canonical_account_email"])
VALID_DEVELOPER_DISPLAY_MODES = {
    str(value) for value in _SERVICE_CONFIG["valid_developer_display_modes"]
}
_FRONTIER_CONFIG = _SERVICE_CONFIG["frontier"]
FRONTIER_STOREFRONT_TYPE = int(_FRONTIER_CONFIG["storefront_type"])
FRONTIER_ACORN_CURRENCY_TYPE = int(_FRONTIER_CONFIG["acorn_currency_type"])
FRONTIER_PURCHASE_CURRENCY_TYPE = int(_FRONTIER_CONFIG["purchase_currency_type"])
FRONTIER_TIER_COUNT = int(_FRONTIER_CONFIG["tier_count"])
FRONTIER_ACORNS_PER_TIER = int(_FRONTIER_CONFIG["acorns_per_tier"])
FRONTIER_TIER_TOKEN_PRICE = int(_FRONTIER_CONFIG["tier_token_price"])
FRONTIER_ELITE_PRICE = int(_FRONTIER_CONFIG["elite_price"])
FRONTIER_ELITE_ITEM_ID = int(_FRONTIER_CONFIG["elite_item_id"])
FRONTIER_XP_BOOSTS = {
    int(key): float(value) for key, value in _FRONTIER_CONFIG["xp_boosts"].items()
}
_LARGE_TOKEN_REWARDS = _SERVICE_CONFIG["large_token_rewards"]
TOKEN_REWARD_MIN = int(_LARGE_TOKEN_REWARDS["minimum"])
TOKEN_REWARD_MAX = int(_LARGE_TOKEN_REWARDS["maximum"])
TOKEN_REWARD_STEP = int(_LARGE_TOKEN_REWARDS["step"])

_STOREFRONT_CONFIG = load_version_json(API_VERSION, "storefronts.json", dict)
STOREFRONT_CURRENCY_TYPES = int_keyed_dict(
    _STOREFRONT_CONFIG["currency_types_by_storefront"], filename="storefronts.json"
)
STOREFRONT_TYPE_NAMES = dict(_STOREFRONT_CONFIG["storefront_type_names"])
CURRENCY_TYPE_NAMES = dict(_STOREFRONT_CONFIG["currency_type_names"])
STOREFRONT_BALANCE_AWARDS = int_keyed_dict(
    _STOREFRONT_CONFIG["balance_awards"], filename="storefronts.json"
)
COMMERCE_CATALOG = tuple(_STOREFRONT_CONFIG["commerce_catalog"])

GAME_INSTANCES_SETTING = f"{API_VERSION}.game_instances"
CHAT_THREADS_SETTING = f"{API_VERSION}.chat_threads"
INVENTIONS_SETTING = f"{API_VERSION}.inventions"
GROUPS_SETTING = f"{API_VERSION}.groups"
DEFAULT_PLAYER_SETTINGS = load_version_json(
    API_VERSION, "default_player_settings.json", dict
)
TUTORIAL_PREFERENCE_DEFAULTS = load_version_json(
    API_VERSION, "tutorial_preferences.json", dict
)
TUTORIAL_PREFERENCE_KEYS = frozenset(TUTORIAL_PREFERENCE_DEFAULTS)

_REPORT_CATEGORIES = load_version_json(API_VERSION, "report_categories.json", dict)
PLAYER_REPORT_CATEGORY_MAP = int_keyed_dict(
    _REPORT_CATEGORIES["player"], filename="report_categories.json"
)
INVENTION_REPORT_CATEGORY_MAP = int_keyed_dict(
    _REPORT_CATEGORIES["invention"], filename="report_categories.json"
)

def _large_token_award(*identity: Any) -> int:
    """Return a stable, visibly large award for the same reward opportunity."""
    digest = hashlib.sha256(
        "|".join(str(value) for value in identity).encode("utf-8")
    ).digest()
    steps = ((TOKEN_REWARD_MAX - TOKEN_REWARD_MIN) // TOKEN_REWARD_STEP) + 1
    return TOKEN_REWARD_MIN + (
        int.from_bytes(digest[:8], "big") % steps
    ) * TOKEN_REWARD_STEP

# Version-owned Charades and generated-name content.
_CHARADES_WORD_DATA = load_version_json(API_VERSION, "charades_words.json", list)
CHARADES_WORDS = tuple(
    (str(item["word"]), int(item["difficulty"])) for item in _CHARADES_WORD_DATA
)
_GENERATED_NAME_DATA = load_version_json(API_VERSION, "generated_names.json", dict)
GENERATED_NAME_NOUNS = tuple(str(value) for value in _GENERATED_NAME_DATA["nouns"])
GENERATED_NAME_ADJECTIVES = tuple(
    str(value) for value in _GENERATED_NAME_DATA["adjectives"]
)

# Process-local SignalR connections keyed by RecNet player ID.
HUB_TRANSPORT = "signalr-hub-v1"
PRESENCE_TRANSPORT = "presence-heartbeat-v3"
_CHAT_LOCK = threading.RLock()
_EVENT_LOCK = threading.RLock()
_PLAYER_STATE_LOCK = threading.RLock()
_NOTIFICATION_LOCK = threading.RLock()
_INVENTION_LOCK = threading.RLock()
_COMMUNITY_BOARD_THUMBNAIL_LOCK = threading.Lock()

HUB_NEGOTIATION_MAX_AGE_SECONDS = 10 * 60
PRESENCE_ONLINE_MAX_AGE_SECONDS = 90

# Version-owned consumable catalog and category limits.
_CONSUMABLE_DATA = load_version_json(API_VERSION, "consumables.json", dict)
BUILD_CONSUMABLES = tuple(
    (
        str(item["name"]),
        str(item["consumable_item_desc"]),
        int(item["category"]),
    )
    for item in _CONSUMABLE_DATA["items"]
)
BUILD_CONSUMABLE_LIMITS = {
    int(category): (int(config["limit_count"]), int(config["limit_type"]))
    for category, config in _CONSUMABLE_DATA["limits"].items()
}

_MEDIA_CONFIG = load_version_json(API_VERSION, "media_config.json", dict)
DEFAULT_IMAGE_NAME = str(_MEDIA_CONFIG["default_image_name"])
COMMUNITY_BOARD_VIDEO_EXTENSIONS = {
    str(value).casefold() for value in _MEDIA_CONFIG["community_board_video_extensions"]
}
_COMMUNITY_BOARD_THUMBNAIL_CONFIG = _MEDIA_CONFIG["community_board_thumbnail"]
COMMUNITY_BOARD_THUMBNAIL_POSITIONS = tuple(
    float(value) for value in _COMMUNITY_BOARD_THUMBNAIL_CONFIG["candidate_positions"]
)
COMMUNITY_BOARD_THUMBNAIL_MIN_LUMA = float(
    _COMMUNITY_BOARD_THUMBNAIL_CONFIG["minimum_luma"]
)
COMMUNITY_BOARD_THUMBNAIL_CACHE_VERSION = int(
    _COMMUNITY_BOARD_THUMBNAIL_CONFIG["cache_version"]
)

# Video metadata is version-owned JSON; unlisted supported files are auto-added.
COMMUNITY_BOARD_VIDEO_CATALOG = load_version_json(
    API_VERSION, "community_board_videos.json", list
)
_PROGRESSION_CONFIG = load_version_json(API_VERSION, "progression.json", dict)
MAX_PLAYER_LEVEL = int(_PROGRESSION_CONFIG["max_player_level"])
STARTING_PLAYER_LEVEL = int(_PROGRESSION_CONFIG["starting_player_level"])
_LEVEL_CURVE = _PROGRESSION_CONFIG["level_curve"]
_OBJECTIVE_XP_CONFIG = _PROGRESSION_CONFIG["objective_xp"]


def _required_xp_for_level(level: int) -> int:
    """Return the non-cumulative XP required to enter ``level``."""
    if level <= 1:
        return 0
    step = level - 1
    return (
        int(_LEVEL_CURVE["base"])
        + (int(_LEVEL_CURVE["linear"]) * step)
        + (int(_LEVEL_CURVE["quadratic"]) * step * step)
    )


LEVEL_PROGRESSION_MAPS = [
    {"Level": level, "RequiredXp": _required_xp_for_level(level)}
    for level in range(1, MAX_PLAYER_LEVEL + 1)
]


def _total_player_xp(level: int, within_level_xp: int) -> int:
    level = max(1, min(int(level), MAX_PLAYER_LEVEL))
    completed = sum(_required_xp_for_level(value) for value in range(2, level + 1))
    if level >= MAX_PLAYER_LEVEL:
        return completed
    return completed + max(0, int(within_level_xp))


def _player_level_progress(total_xp: int) -> tuple[int, int]:
    remaining = max(0, int(total_xp))
    for next_level in range(2, MAX_PLAYER_LEVEL + 1):
        required = _required_xp_for_level(next_level)
        if remaining < required:
            return next_level - 1, remaining
        remaining -= required
    return MAX_PLAYER_LEVEL, 0


def _objective_xp_award(level: int, additional_xp: int) -> int:
    # XP awards taper gradually from 100% at level 1 to 42% at level 30.
    # There is intentionally no daily counter or daily XP cap.
    multiplier = max(
        float(_OBJECTIVE_XP_CONFIG["minimum_multiplier"]),
        1.0 - (
            float(_OBJECTIVE_XP_CONFIG["per_level_taper"])
            * (max(1, min(level, MAX_PLAYER_LEVEL)) - 1)
        ),
    )
    return max(
        int(_OBJECTIVE_XP_CONFIG["minimum_award"]),
        int(round(
            (int(_OBJECTIVE_XP_CONFIG["base"]) + max(0, int(additional_xp)))
            * multiplier
        )),
    )


# Version-owned quest reward catalog.
_QUEST_REWARD_DATA = load_version_json(API_VERSION, "quest_rewards.json", dict)
QUEST_RANK_REWARDS = {
    int(context): (
        str(reward["name"]),
        str(reward["avatar_item_desc"]),
        int(reward["rarity"]),
    )
    for context, reward in _QUEST_REWARD_DATA["rank_rewards"].items()
}
QUEST_CONSUMABLE_REWARDS = {
    int(context): (str(reward["name"]), str(reward["consumable_item_desc"]))
    for context, reward in _QUEST_REWARD_DATA["consumable_rewards"].items()
}

DAILY_OBJECTIVES_DEFAULTS = load_version_json(
    API_VERSION, "daily_objectives.json", list
)
_AVATAR_DEFAULTS = load_version_json(API_VERSION, "avatar_defaults.json", dict)
DEFAULT_AVATAR = dict(_AVATAR_DEFAULTS["existing_account"])
NEW_ACCOUNT_DEFAULT_AVATAR = {
    **DEFAULT_AVATAR,
    **dict(_AVATAR_DEFAULTS["new_account"]),
}

# Version-owned avatar catalog.
_AVATAR_ITEM_DATA = load_version_json(API_VERSION, "avatar_items.json", dict)
BUILD_AVATAR_ITEMS = tuple(
    (str(item["avatar_item_desc"]), int(item["category"]))
    for item in _AVATAR_ITEM_DATA["items"]
)
BUILD_AVATAR_ITEM_NAMES = tuple(
    str(item["asset_name"]) for item in _AVATAR_ITEM_DATA["items"]
)
DEFAULT_UNLOCKED_AVATAR_ITEM_DESCS = tuple(
    str(value)
    for value in _AVATAR_ITEM_DATA["default_unlocked_outfit_selections"]
)


def _normalize_avatar_item_desc(value: str) -> str:
    """Return the exact four-part OutfitSelection.ToString descriptor."""
    desc = str(value or "")
    return desc if desc.count(",") == 3 else f"{desc},,,"


# Version-owned equipment skin catalog.
_EQUIPMENT_SKIN_DATA = load_version_json(API_VERSION, "equipment_skins.json", dict)
BUILD_EQUIPMENT_SKINS = tuple(
    (str(item["prefab_name"]), str(item["modification_guid"]))
    for item in _EQUIPMENT_SKIN_DATA["skins"]
)
BUILD_EQUIPMENT_SKIN_ASSET_NAMES = tuple(
    str(item["asset_name"]) for item in _EQUIPMENT_SKIN_DATA["skins"]
)


def _equipment_skin_friendly_name(skin_asset_name: str, prefab: str) -> str:
    """Create a readable store label from a SkinData asset name."""
    equipment_key = prefab.strip("[]")
    remainder = skin_asset_name
    if remainder.casefold().startswith(equipment_key.casefold()):
        remainder = remainder[len(equipment_key):]
    style_key = "_".join(
        part
        for part in remainder.strip("_").split("_")
        if part and part.casefold() != "skin"
    )

    def humanize(value: str) -> str:
        value = value.replace("PSPLUS", "PS Plus").replace("SciFi", "Sci-Fi")
        value = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value.replace("_", " "))
        words = [
            word.capitalize() if word.islower() or word.isupper() else word
            for word in value.split()
        ]
        return (
            " ".join(words)
            .replace("Jackolantern", "Jack-o'-lantern")
            .replace("Coop", "Co-op")
            .replace("Ps Plus", "PS Plus")
        )

    equipment_name = humanize(equipment_key)
    style_name = humanize(style_key)
    return f"{style_name} {equipment_name} Skin" if style_name else f"{equipment_name} Skin"


# Version-owned Coach room catalog.
BUILD_COACH_ROOMS = tuple(
    room
    for room in load_version_json(API_VERSION, "coach_rooms.json", list)
    if str(room.get("n") or "").casefold() != "calibration"
)

# Room-card textures are loaded from version-owned JSON.
COACH_ROOM_IMAGE_TEXTURES = load_version_json(
    API_VERSION, "coach_room_image_textures.json", dict
)

ROOM_IMAGE_DATA_DIR = Path(__file__).resolve().parents[1] / "DATA" / "IMAGES" / "RR"
COMMUNITY_BOARD_VIDEO_DIR = Path(__file__).resolve().parents[1] / "DATA" / "Videos"
COMMUNITY_BOARD_THUMBNAIL_SUBDIR = Path("IMAGES") / "RR"

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
    module_path = Path(__file__).with_name("9march2017.py")
    spec = importlib.util.spec_from_file_location("recroom_api_9march2017_shared_for_25april2019", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load 9march2017 adapter.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _retarget_module(module)
    return module


_SHARED = _load_shared_adapter()
_BASE = getattr(_SHARED, "_BASE")
_PLATFORM_BASE = getattr(_BASE, "_PLATFORM_BASE")


def _local_profile_id(request: Request, context=None) -> int:
    raw_id = request.headers.get("X-Rec-Room-Profile") or request.headers.get("x-rec-room-profile")
    try:
        player_id = int(raw_id or 0)
    except Exception:
        player_id = 0
    if player_id > 0:
        return player_id

    if context is not None:
        row = context.player_from_request(request, API_VERSION)
        if row is not None:
            try:
                state = json.loads(row["state_json"] or "{}")
            except Exception:
                state = {}
            player_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
            if player_id > 0:
                return player_id

    raise HTTPException(status_code=401, detail="Authenticated player is required.")


def _authenticated_player(
    request: Request,
    context,
    *,
    allow_account_sanction: bool = False,
):
    row = context.player_from_request(request, API_VERSION)
    if row is None:
        raise HTTPException(status_code=401, detail="Authenticated player is required.")
    request_is_sanctioned = False
    if allow_account_sanction:
        try:
            context.assert_request_not_banned(request, API_VERSION)
        except HTTPException as exc:
            if exc.status_code != 403:
                raise
            request_is_sanctioned = True
    else:
        context.assert_player_not_banned(row["player_id"])
    player = dict(row)
    try:
        player["state"] = json.loads(player.get("state_json") or "{}")
    except Exception:
        player["state"] = {}
    player["_request_is_sanctioned"] = request_is_sanctioned
    _ensure_minimum_player_level(player, context)
    return player


def _maintenance_room_lock_active(context) -> bool:
    state = context.get_maintenance_state()
    if not bool(state.get("active")):
        return False
    deadline = _parse_recnet_datetime(state.get("starts_at_utc"))
    return deadline is not None and deadline <= datetime.now(timezone.utc)


def _account_room_lock_active(player: dict[str, Any], context) -> bool:
    if bool(player.get("_request_is_sanctioned")) or bool(player.get("is_banned")):
        return True
    return context.active_player_sanction(str(player["player_id"])) is not None


def _player_room_lock_active(player: dict[str, Any], context) -> bool:
    return _account_room_lock_active(player, context) or _maintenance_room_lock_active(
        context
    )


def _assert_invention_publishing_allowed(context, player: dict[str, Any]) -> None:
    if context.has_player_restriction(
        str(player["player_id"]),
        "invention_publishing",
    ):
        raise HTTPException(
            status_code=403,
            detail="Invention creation, editing, and publishing are temporarily restricted.",
        )


def _filter_user_text(
    context,
    value: str,
    *,
    policy: str,
    field_context: str,
    player: dict[str, Any] | None = None,
) -> str:
    try:
        result = context.filter_user_text(
            value,
            policy=policy,
            field_context=field_context,
            player_id=str(player["player_id"]) if player is not None else None,
            source_version=API_VERSION,
        )
    except ProhibitedProfileText as exc:
        raise HTTPException(status_code=400, detail="Text did not pass validation.") from exc
    except ContentFilterError as exc:
        raise HTTPException(status_code=503, detail="Text safety service is unavailable.") from exc
    return result.output_text


def _user_text_is_pure(
    context,
    value: str,
    *,
    field_context: str,
    player: dict[str, Any],
) -> bool:
    try:
        result = context.filter_user_text(
            value,
            policy="censor",
            field_context=field_context,
            player_id=str(player["player_id"]),
            source_version=API_VERSION,
        )
    except ContentFilterError as exc:
        raise HTTPException(
            status_code=503,
            detail="Text safety service is unavailable.",
        ) from exc
    return not result.changed


def _submit_canonical_report(
    *,
    reporter: dict[str, Any],
    target_type: str,
    target_id: Any,
    raw_category: Any,
    canonical_category: str,
    category_schema: str,
    details: str,
    room_id: Any | None,
    game_session_id: Any | None,
    source_endpoint: str,
    source_payload: dict[str, Any],
    context,
) -> dict[str, Any]:
# Preserve report details verbatim as administrator-only evidence.
    public_details = details
    safe_payload = {
        str(key): value
        for key, value in source_payload.items()
        if str(key).casefold() not in {
            "details",
            "authorization",
            "token",
            "accesstoken",
            "password",
        }
        and isinstance(value, (str, int, float, bool, type(None)))
    }
    return context.create_moderation_report(
        reporter_player_id=str(reporter["player_id"]),
        target_type=target_type,
        target_id=str(target_id),
        canonical_category=canonical_category,
        raw_category=raw_category,
        category_schema=category_schema,
        public_details=public_details,
        raw_details=details,
        room_id=str(room_id) if room_id not in (None, "", 0, "0") else None,
        game_session_id=(
            str(game_session_id)
            if game_session_id not in (None, "", 0, "0")
            else None
        ),
        source_version=API_VERSION,
        source_endpoint=source_endpoint,
        source_schema=category_schema,
        source_payload=safe_payload,
        evidence_status="unavailable",
    )


def _ensure_minimum_player_level(player: dict[str, Any], context) -> None:
    """Backfill the April 2019 baseline without resetting earned progress."""
    if bool(player.get("is_coach", False)):
        return
    current_level = max(1, int(player.get("canonical_level") or 1))
    if current_level >= STARTING_PLAYER_LEVEL:
        return
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET canonical_level = ?, canonical_xp = 0,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND is_coach = 0 AND canonical_level < ?
            """,
            (STARTING_PLAYER_LEVEL, player["player_id"], STARTING_PLAYER_LEVEL),
        )
    player["canonical_level"] = STARTING_PLAYER_LEVEL
    player["canonical_xp"] = 0


def _assert_account_creation_allowed(
    context, request: Request, identities: list[tuple[str, Any]]
) -> None:
    checker = getattr(context, "assert_account_creation_allowed", None)
    if callable(checker):
        checker(request, API_VERSION, identities)
        return
    combined = list(identities)
    request_pairs = getattr(context, "request_identity_pairs", None)
    if callable(request_pairs):
        combined.extend(request_pairs(request, API_VERSION))
    context.assert_identities_not_banned(combined)


def _canonical_player_setting_key(kind: str, player_id: str) -> str:
    return f"{kind}.{player_id}"


def _tutorial_player_setting_key(player_id: str) -> str:
# Keep tutorial progress version-local so other builds cannot suppress OOBE.
    return _canonical_player_setting_key(
        f"{API_VERSION}_tutorial_preferences", player_id
    )


def _initialize_25april2019_tutorial(player, state: dict[str, Any], context) -> None:
    _BASE._set_json_setting(
        context,
        _tutorial_player_setting_key(str(player["player_id"])),
        dict(TUTORIAL_PREFERENCE_DEFAULTS),
    )
    state["tutorial_initialized"] = True


def _load_25april2019_version_state(
    player, context
) -> tuple[dict[str, Any], bool]:
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT state_json
            FROM player_version_state
            WHERE player_id = ? AND api_version = ?
            """,
            (player["player_id"], API_VERSION),
        ).fetchone()
    if row is None:
        return {}, False
    try:
        state = json.loads(row["state_json"] or "{}")
    except Exception:
        state = {}
    return (state if isinstance(state, dict) else {}), True


def _bootstrap_25april2019_version_state(
    player,
    context,
    *,
    identity_key: str | None = None,
    platform: int | None = None,
    platform_id: str | None = None,
) -> tuple[dict[str, Any], bool]:
    state, existed = _load_25april2019_version_state(player, context)
    if existed:
        player["state"] = state
        return state, False

    legacy_id = _allocate_legacy_player_id(context)
    username = str(player.get("username") or player.get("display_name") or f"Player{legacy_id}")
    state = {
        "legacy_player_id": legacy_id,
        "recnet_id": legacy_id,
        "identity_key": identity_key or f"recnet:{legacy_id}",
        "name": str(player.get("display_name") or username),
        "bio": DEFAULT_PLAYER_BIO,
    }
    if platform is not None:
        state["platform"] = int(platform)
    if platform_id:
        state["platform_id"] = str(platform_id)

    context.ensure_player_version_state(player["player_id"], API_VERSION, state)
    _initialize_25april2019_tutorial(player, state, context)
    _persist_player_state(player, state, context)
    player["state"] = state
    return state, True


def _initialize_new_account_avatar(player, context) -> None:
    key = _canonical_player_setting_key("player_avatar", str(player["player_id"]))
    _BASE._set_json_setting(context, key, dict(NEW_ACCOUNT_DEFAULT_AVATAR))


def _bool_value(value: Any, *, default: bool = False) -> bool:
    """Parse JSON and form booleans without treating the string 'false' as true."""
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off", "", "null", "none"}:
            return False
        return default
    return bool(value)


def _enum_value(value: Any, names: dict[str, int], *, default: int) -> int:
    """Accept Json.NET enum names as well as their integer representation."""
    if isinstance(value, str):
        folded = value.strip().casefold()
        if folded in names:
            return names[folded]
        try:
            return int(folded)
        except ValueError:
            return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _payload_enum(
    payload: dict[str, Any], *keys: str, names: dict[str, int], default: int
) -> int:
    for key in keys:
        if key in payload:
            return _enum_value(payload[key], names, default=default)
    return default


def _allocate_legacy_player_id(context) -> int:
    # Profile ID 1 is reserved for the virtual Coach profile used by every
    # Rec Room Original in this build.
    allocated = int(_PLATFORM_BASE._allocate_legacy_player_id(context))
    if allocated == 1:
        allocated = int(_PLATFORM_BASE._allocate_legacy_player_id(context))
    return allocated


def _enforce_private_verified_account(player, context) -> None:
    """Keep submitted contact details out of storage and mark the account verified."""
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players
            SET email = ?, verified = 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND is_coach = 0
            """,
            (CANONICAL_ACCOUNT_EMAIL, player["player_id"]),
        )


def _find_player_by_username(context, username: str) -> dict[str, Any] | None:
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND lower(p.username) = ?
            LIMIT 1
            """,
            (API_VERSION, username.lower()),
        ).fetchone()
    if row is None:
        return None
    player = {key: row[key] for key in row.keys() if key != "state_json"}
    try:
        player["state"] = json.loads(row["state_json"] or "{}")
    except Exception:
        player["state"] = {}
    if bool((player.get("state") or {}).get("deleted", False)):
        return None
    return player


def _find_canonical_player_by_username(context, username: str) -> dict[str, Any] | None:
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM players
            WHERE lower(username) = ?
            LIMIT 1
            """,
            (username.lower(),),
        ).fetchone()
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _find_player_by_platform_25april2019(context, *, platform: int, platform_id: str) -> dict[str, Any] | None:
    player = context.find_player_by_identity("account_id", f"steam:{platform_id}" if platform == 0 else f"platform:{platform}:{platform_id}")
    if player is None:
        player = context.find_player_by_identity("account_id", f"platform:{platform}:{platform_id}")
    if player is None:
        with context.db.connection() as conn:
            row = conn.execute(
                """
                SELECT p.*, pvs.state_json
                FROM players AS p
                JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
                WHERE pvs.api_version = ?
                  AND CAST(json_extract(pvs.state_json, '$.platform') AS INTEGER) = ?
                  AND json_extract(pvs.state_json, '$.platform_id') = ?
                """,
                (API_VERSION, platform, platform_id),
            ).fetchone()
            if row:
                player = {key: row[key] for key in row.keys() if key != "state_json"}
                try:
                    player["state"] = json.loads(row["state_json"] or "{}")
                except Exception:
                    player["state"] = {}
    if player is not None and not isinstance(player.get("state"), dict):
        with context.db.connection() as conn:
            state_row = conn.execute(
                "SELECT state_json FROM player_version_state WHERE player_id = ? AND api_version = ?",
                (player["player_id"], API_VERSION),
            ).fetchone()
        try:
            player["state"] = json.loads(state_row["state_json"] or "{}") if state_row is not None else {}
        except Exception:
            player["state"] = {}
    return player


def _find_player_by_legacy_id_25april2019(context, legacy_id: int) -> dict[str, Any] | None:
    if legacy_id == 1:
        # Coach is a real client-visible creator for every RRO, but does not
        # need a mutable account row in the private player database.
        return {
            "player_id": COACH_PLAYER_UUID,
            "username": "Coach",
            "display_name": "Coach",
            "canonical_xp": 14_500,
            "canonical_level": 30,
            "permissions": ["DEV"],
            "state": {
                "legacy_player_id": 1,
                "name": "Coach",
                "bio": "Welcome to Rec Room!",
                "subscriber_count": int(
                    _BASE._get_json_setting(context, f"{API_VERSION}:coach_subscriber_count", 0) or 0
                ),
                "subscribed_count": 0,
            },
        }
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND CAST(json_extract(pvs.state_json, '$.legacy_player_id') AS INTEGER) = ?
            LIMIT 1
            """,
            (API_VERSION, legacy_id),
        ).fetchone()
    if row is None:
        return None
    player = {key: row[key] for key in row.keys() if key != "state_json"}
    try:
        player["state"] = json.loads(row["state_json"] or "{}")
    except Exception:
        player["state"] = {}
    if bool((player.get("state") or {}).get("deleted", False)):
        return None
    return player


def _serialize_profile_25april2019(player: dict[str, Any]) -> dict[str, Any]:
    state = player.get("state") or {}
    cheer_counts = state.get("cheer_counts")
    if not isinstance(cheer_counts, dict):
        cheer_counts = {}
    legacy_id = int(state.get("legacy_player_id") or 1)
    username = player.get("username") or state.get("name") or f"Player{legacy_id}"
    display_name = player.get("display_name") or state.get("name") or username

    # Handle platform info
    platform = state.get("platform")
    if platform is None:
        platform = 0
    else:
        platform = int(platform)
    platform_id_str = str(state.get("platform_id") or "0")
    try:
        platform_id = int(platform_id_str)
    except ValueError:
        platform_id = 0

    canonical_level = max(
        STARTING_PLAYER_LEVEL,
        min(int(player.get("canonical_level") or STARTING_PLAYER_LEVEL), MAX_PLAYER_LEVEL),
    )
    canonical_xp = max(0, int(player.get("canonical_xp") or 0))
    if canonical_level >= MAX_PLAYER_LEVEL:
        canonical_xp = 0
    else:
        canonical_xp = min(canonical_xp, _required_xp_for_level(canonical_level + 1) - 1)

    is_junior = bool(state.get("is_junior", False))

    return {
        "Id": legacy_id,
        "Username": username,
        "DisplayName": display_name,
        "Bio": str(state.get("bio") or ""),
        "XP": canonical_xp,
        "Level": canonical_level,
        "RegistrationStatus": 2,
        # Every profile in this restored private service has the exact
        # client's developer entitlement, including profiles created later.
        "Developer": True,
        "CanReceiveInvites": True,
# Use unique filenames so profile-image DTOs and URLs invalidate immediately.
        "ProfileImageName": str(state.get("profile_image_name") or DEFAULT_IMAGE_NAME),
        "JuniorProfile": is_junior,
        "ForceJuniorImages": is_junior,
        "PendingJunior": False,
        "AvoidJuniors": bool(state.get("avoid_juniors", False)),
        "HasBirthday": bool(state.get("has_birthday", True)),
        "PlayerReputation": {
            "Noteriety": max(0.0, float(state.get("noteriety", 0.0) or 0.0)),
            "SelectedCheer": state.get("selected_cheer"),
            "CheerCredit": CHEER_BALANCE,
# Positive cheer counts are badge ownership flags; all five start unlocked.
            "CheerGeneral": max(1, int(cheer_counts.get("CheerGeneral", 0) or 0)),
            "CheerHelpful": max(1, int(cheer_counts.get("CheerHelpful", 0) or 0)),
            "CheerCreative": max(1, int(cheer_counts.get("CheerCreative", 0) or 0)),
            "CheerGreatHost": max(1, int(cheer_counts.get("CheerGreatHost", 0) or 0)),
            "CheerSportsman": max(1, int(cheer_counts.get("CheerSportsman", 0) or 0)),
            "SubscriberCount": max(0, int(state.get("subscriber_count", 0) or 0)),
            "SubscribedCount": max(0, int(state.get("subscribed_count", 0) or 0))
        },
        "PlatformId": {
            "Platform": platform,
            "PlatformId": platform_id
        }
    }


async def _make_login_response(
    player: dict[str, Any], token: str, context
) -> dict[str, Any]:
    await context.issue_player_session(
        api_version=API_VERSION,
        raw_token=token,
        player_id=str(player["player_id"]),
        legacy_player_id=_legacy_id_for_player(player),
        ttl_seconds=7 * 24 * 60 * 60,
    )
    profile_data = _serialize_profile_25april2019(player)
    return {
        "Player": profile_data,
        "Token": token,
        "Error": "",
        "FirstLoginOfTheDay": True,
        "AnalyticsSessionId": secrets.randbelow(9_000_000_000) + 1_000_000_000,
        "CachedPlatformMask": 0
    }


async def _handle_platform_login(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    platform_id = _BASE._str_field(payload, "PlatformId", "platformId", "platform_id")
    if not platform_id:
        raise HTTPException(status_code=400, detail="PlatformId is required.")
    name = _BASE._str_field(payload, "Name", "name", default=f"Player{platform_id[-4:]}")
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)

    context.assert_identities_not_banned(
        [
            ("account_id", f"steam:{platform_id}" if platform == 0 else f"platform:{platform}:{platform_id}"),
            ("account_id", f"{API_VERSION}:platform:{platform}:{platform_id}"),
        ]
    )

    player = _find_player_by_platform_25april2019(context, platform=platform, platform_id=platform_id)
    if player is None:
        name = _filter_user_text(
            context,
            name,
            policy="reject_profile",
            field_context="profile.platform_name",
        )
        legacy_id = _allocate_legacy_player_id(context)
        player = context.get_or_create_player(
            API_VERSION,
            username=name,
            display_name=name,
            identity_key=f"platform:{platform}:{platform_id}"
        )
        _initialize_new_account_avatar(player, context)
        state = {
            "legacy_player_id": legacy_id,
            "identity_key": f"platform:{platform}:{platform_id}",
            "platform": platform,
            "platform_id": platform_id,
            "name": name,
            "bio": DEFAULT_PLAYER_BIO,
        }
        _initialize_25april2019_tutorial(player, state, context)
    else:
        state, _ = _bootstrap_25april2019_version_state(
            player,
            context,
            identity_key=f"platform:{platform}:{platform_id}",
            platform=platform,
            platform_id=platform_id,
        )
        legacy_id = int(state.get("legacy_player_id") or 1)
        # A platform login refresh must not bypass profile-text validation by
        # silently overwriting the already-reviewed Rec Room profile name.
        name = str(
            state.get("name")
            or player.get("display_name")
            or player.get("username")
            or f"Player{legacy_id}"
        )

    token = f"local-{API_VERSION}-{legacy_id}"
    await _clear_active_game_session_for_login(player, state, context)
    state.update({
        "identity_key": f"platform:{platform}:{platform_id}",
        "platform": platform,
        "platform_id": platform_id,
        "name": name,
        "recnet_id": legacy_id,
        "cached_login_hidden": False,
    })

    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )

    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"steam:{platform_id}"),
            ("account_id", f"{API_VERSION}:platform:{platform}:{platform_id}"),
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )

    player["state"] = state
    return JSONResponse(await _make_login_response(player, token, context))


async def _handle_login_v2(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    profile_id = _BASE._int_field(payload, "profileId", "ProfileId", default=0)
    username = _BASE._str_field(payload, "username", "Username", "usernameOrEmail", "UsernameOrEmail")
    
    player = None
    if profile_id > 0:
        player = _find_player_by_legacy_id_25april2019(context, profile_id)
    elif username:
        player = _find_player_by_username(context, username)
        if player is None:
            player = _find_canonical_player_by_username(context, username)
            if player is not None:
                _bootstrap_25april2019_version_state(player, context)
        
    if player is None:
        return JSONResponse({
            "Player": None,
            "Token": None,
            "Error": "AccountNotFound",
            "FirstLoginOfTheDay": False,
            "AnalyticsSessionId": 0,
            "CachedPlatformMask": 0
        })
        
    legacy_id = int(player.get("state", {}).get("legacy_player_id") or 1)
    token = f"local-{API_VERSION}-{legacy_id}"
    
    state = player.get("state") or {}
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )
        
    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )
    
    player["state"] = state
    return JSONResponse(await _make_login_response(player, token, context))


async def _handle_get_cached_logins(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    platform_id = _BASE._str_field(payload, "PlatformId", "platformId", "platform_id").strip()
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)
# Return only profiles owned by the requesting platform account.
    if not platform_id:
        return JSONResponse([])
    profiles = []
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
            ORDER BY pvs.updated_at DESC
            """,
            (API_VERSION,),
        ).fetchall()
        
    for row in rows:
        player = {key: row[key] for key in row.keys() if key != "state_json"}
        try:
            player["state"] = json.loads(row["state_json"] or "{}")
        except Exception:
            player["state"] = {}
        state = player["state"] if isinstance(player["state"], dict) else {}
        if (
            int(state.get("platform", 0) or 0) != platform
            or str(state.get("platform_id") or "") != platform_id
            or bool(state.get("cached_login_hidden", False))
            or bool(state.get("deleted", False))
        ):
            continue
            
        profile_data = _serialize_profile_25april2019(player)
        profiles.append({
            "Player": profile_data,
            "LastLoginTime": row["updated_at"],
            "RequirePassword": False
        })
        
    return JSONResponse(profiles)


async def _handle_refresh_login(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    state = _player_state(player)
    legacy_id = _legacy_id_for_player(player)
    if legacy_id <= 0:
        raise HTTPException(status_code=409, detail="Player has no 2019 profile ID.")
    token = f"local-{API_VERSION}-{legacy_id}"
    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )
    await context.issue_player_session(
        api_version=API_VERSION,
        raw_token=token,
        player_id=str(player["player_id"]),
        legacy_player_id=legacy_id,
        ttl_seconds=7 * 24 * 60 * 60,
    )
    # Login.RefreshLogin deserializes RefreshLoginResponse, whose only field
    # in this client is Token.
    return JSONResponse({"Token": token})


async def _handle_remove_cached_login(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    state = _player_state(player)
    requested_platform = _BASE._int_field(
        payload, "Platform", "platform", default=int(state.get("platform", 0) or 0)
    )
    requested_platform_id = _BASE._str_field(
        payload,
        "PlatformId",
        "platformId",
        default=str(state.get("platform_id") or ""),
    )
    if (
        requested_platform != int(state.get("platform", 0) or 0)
        or requested_platform_id != str(state.get("platform_id") or "")
    ):
        raise HTTPException(status_code=403, detail="Cached login does not belong to this platform account.")
# Removing a cached profile only hides its chooser entry.
    state["cached_login_hidden"] = True
    _persist_player_state(player, state, context)
    return Response(status_code=204)


async def _handle_avoid_juniors(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    raw = payload.get("AvoidJuniors", payload.get("avoidJuniors"))
    if isinstance(raw, str):
        folded = raw.strip().casefold()
        if folded not in {"true", "false", "1", "0"}:
            raise HTTPException(status_code=400, detail="AvoidJuniors must be a boolean.")
        enabled = folded in {"true", "1"}
    elif isinstance(raw, (bool, int)):
        enabled = bool(raw)
    else:
        raise HTTPException(status_code=400, detail="AvoidJuniors is required.")
    state = _player_state(player)
    state["avoid_juniors"] = enabled
    _persist_player_state(player, state, context)
    return Response(status_code=204)


async def _handle_password_recovery(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    email = _BASE._str_field(payload, "Email", "email").strip()
    if not email or "@" not in email or len(email) > 254:
        raise HTTPException(status_code=400, detail="A valid email address is required.")
# Local passwordless accounts do not persist contact details or send mail.
    return Response(status_code=204)


async def _handle_create_profile(request: Request, context) -> Response:
    owner = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    display_name = _BASE._str_field(payload, "Name", "name").strip()
    if not display_name or len(display_name) > 32:
        raise HTTPException(status_code=400, detail="Name must contain 1 to 32 characters.")
    display_name = _filter_user_text(
        context,
        display_name,
        policy="reject_profile",
        field_context="profile.display_name",
        player=owner,
    )

    owner_state = _player_state(owner)
    platform = int(owner_state.get("platform", 0) or 0)
    platform_id = str(owner_state.get("platform_id") or "").strip()
    if not platform_id:
        raise HTTPException(status_code=409, detail="A platform account is required to create another profile.")

    legacy_id = _allocate_legacy_player_id(context)
    # The 2019 profile chooser displays both fields. New profiles begin with
    # the submitted name as both Username and DisplayName.
    username = display_name
    _assert_account_creation_allowed(
        context,
        request,
        [("username_lower", username.casefold())],
    )
    context.assert_username_available(username)
    profile = context.get_or_create_player(
        API_VERSION,
        username=username,
        display_name=username,
        identity_key=f"profile:{owner['player_id']}:{legacy_id}",
    )
    _initialize_new_account_avatar(profile, context)
    _enforce_private_verified_account(profile, context)
    state = {
        "legacy_player_id": legacy_id,
        "recnet_id": legacy_id,
        "identity_key": f"profile:{owner['player_id']}:{legacy_id}",
        "parent_profile_player_id": str(owner["player_id"]),
        "platform": platform,
        "platform_id": platform_id,
        "name": username,
        "bio": DEFAULT_PLAYER_BIO,
        "has_birthday": False,
        "is_junior": False,
        "cached_login_hidden": False,
    }
    _initialize_25april2019_tutorial(profile, state, context)
    _persist_player_state(profile, state, context)
    context.record_player_identities(
        profile["player_id"],
        [
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )
    profile["state"] = state
    return JSONResponse(_serialize_profile_25april2019(profile))


async def _handle_delete_profile(request: Request, context) -> Response:
    owner = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    target_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    # Password is part of the exact wire shape, but local accounts are
    # intentionally passwordless and submitted secrets are never retained.
    _BASE._str_field(payload, "Password", "password", default="")
    if target_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    if target_id == _legacy_id_for_player(owner):
        raise HTTPException(status_code=400, detail="The active profile cannot delete itself.")

    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target is None or target_id == 1:
        raise HTTPException(status_code=404, detail="Player not found.")
    owner_state = _player_state(owner)
    target_state = _player_state(target)
    if (
        int(target_state.get("platform", 0) or 0) != int(owner_state.get("platform", 0) or 0)
        or str(target_state.get("platform_id") or "") != str(owner_state.get("platform_id") or "")
    ):
        raise HTTPException(status_code=403, detail="The profile does not belong to this platform account.")

    # Preserve referential integrity for rooms, messages and leaderboards while
    # removing the profile from all login and public-profile paths.
    target_state["deleted"] = True
    target_state["cached_login_hidden"] = True
    target_state.pop("game_session", None)
    _persist_player_state(target, target_state, context)
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_birthday_update(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    raw_birthday = _BASE._str_field(
        payload, "BirthdayDateString", "birthdayDateString"
    ).strip()
    try:
        birthday = datetime.strptime(raw_birthday, "%m/%d/%Y").date()
    except (TypeError, ValueError):
        return JSONResponse({
            "Success": False,
            "Message": "Birthday must use MM/dd/yyyy.",
            "MustRestart": False,
            "IsJunior": False,
        })
    today = datetime.now(timezone.utc).date()
    age = today.year - birthday.year - (
        (today.month, today.day) < (birthday.month, birthday.day)
    )
    if birthday > today or age > 120:
        return JSONResponse({
            "Success": False,
            "Message": "Birthday is outside the supported range.",
            "MustRestart": False,
            "IsJunior": False,
        })

    state = _player_state(player)
    previous_junior = bool(state.get("is_junior", False))
    is_junior = age < 13
    state["has_birthday"] = True
    state["is_junior"] = is_junior
    # Persist only the derived birthday and junior flags.
    _persist_player_state(player, state, context)
    await _broadcast_profile_update(_legacy_id_for_player(player), context)
    return JSONResponse({
        "Success": True,
        "Message": "",
        "MustRestart": previous_junior != is_junior,
        "IsJunior": is_junior,
    })


async def _handle_generated_name_options(request: Request, context) -> Response:
    _authenticated_player(request, context)
    return JSONResponse({
        "Nouns": list(GENERATED_NAME_NOUNS),
        "Adjectives": list(GENERATED_NAME_ADJECTIVES),
    })


async def _handle_reputation_update(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    reputation_delta = _BASE._int_field(
        payload, "reputationDelta", "ReputationDelta", default=0
    )
    blocked_duration = _BASE._int_field(
        payload, "blockedDuration", "BlockedDuration", default=0
    )
    if blocked_duration < 0:
        raise HTTPException(status_code=400, detail="blockedDuration cannot be negative.")
    state = _player_state(player)
    state["noteriety"] = max(
        0.0, float(state.get("noteriety", 0.0) or 0.0) + reputation_delta
    )
    if blocked_duration > 0:
        state["reputation_blocked_until"] = _format_recnet_datetime(
            datetime.now(timezone.utc) + timedelta(seconds=blocked_duration)
        )
    _persist_player_state(player, state, context)
    await _broadcast_profile_update(_legacy_id_for_player(player), context)
    return Response(status_code=204)


async def _handle_reputation_heal(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    good_minutes = _BASE._int_field(
        payload, "GoodKarmaMinutes", "goodKarmaMinutes", default=0
    )
    if good_minutes < 0:
        raise HTTPException(status_code=400, detail="GoodKarmaMinutes cannot be negative.")
    state = _player_state(player)
    state["good_karma_minutes"] = max(
        0, int(state.get("good_karma_minutes", 0) or 0) + good_minutes
    )
    state["noteriety"] = max(
        0.0, float(state.get("noteriety", 0.0) or 0.0) - good_minutes
    )
    _persist_player_state(player, state, context)
    await _broadcast_profile_update(_legacy_id_for_player(player), context)
    return Response(status_code=204)


async def _handle_create_account(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    username = _BASE._str_field(payload, "username", "Username", default="")
    # Parse compatibility fields, but deliberately never persist their values.
    _BASE._str_field(payload, "email", "Email", default="")
    _BASE._str_field(payload, "phone", "Phone", "phoneNumber", "PhoneNumber", default="")
    
    if not username:
        legacy_id = _allocate_legacy_player_id(context)
        username = f"Player{legacy_id}"
    else:
        legacy_id = _allocate_legacy_player_id(context)
    username = _filter_user_text(
        context,
        username,
        policy="reject_profile",
        field_context="profile.username",
    )

    _assert_account_creation_allowed(
        context,
        request,
        [("username_lower", username)],
    )
        
    context.assert_username_available(username)
    player = context.get_or_create_player(
        API_VERSION,
        username=username,
        display_name=username,
        identity_key=f"recnet:{legacy_id}"
    )
    _initialize_new_account_avatar(player, context)
    
    _enforce_private_verified_account(player, context)
    state = {
        "legacy_player_id": legacy_id,
        "identity_key": f"recnet:{legacy_id}",
        "name": username,
        "bio": DEFAULT_PLAYER_BIO,
    }
    _initialize_25april2019_tutorial(player, state, context)
    
    token = f"local-{API_VERSION}-{legacy_id}"
    
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )
        
    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )
    
    player["state"] = state
    return JSONResponse(await _make_login_response(player, token, context))


async def _handle_create_account_v2(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    username = _BASE._str_field(payload, "username", "Username", default="")
    _BASE._str_field(payload, "email", "Email", default="")
    _BASE._str_field(payload, "phone", "Phone", "phoneNumber", "PhoneNumber", default="")
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)
    platform_id = _BASE._str_field(payload, "PlatformId", "platformId", "platform_id")
    if not platform_id:
        # Account creation can precede platform identity assignment.
        legacy_id = _allocate_legacy_player_id(context)
        username = username or f"Player{legacy_id}"
        username = _filter_user_text(
            context,
            username,
            policy="reject_profile",
            field_context="profile.username",
        )
        _assert_account_creation_allowed(
            context,
            request,
            [("username_lower", username)],
        )
        context.assert_username_available(username)
        player = context.get_or_create_player(
            API_VERSION,
            username=username,
            display_name=username,
            identity_key=f"recnet:{legacy_id}",
        )
        _initialize_new_account_avatar(player, context)
        _enforce_private_verified_account(player, context)
        token = f"local-{API_VERSION}-{legacy_id}"
        state = {
            "legacy_player_id": legacy_id,
            "recnet_id": legacy_id,
            "identity_key": f"recnet:{legacy_id}",
            "name": username,
            "bio": DEFAULT_PLAYER_BIO,
        }
        _initialize_25april2019_tutorial(player, state, context)
        _persist_player_state(player, state, context)
        context.record_player_identities(player["player_id"], [
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ])
        player["state"] = state
        return JSONResponse(await _make_login_response(player, token, context))
        
    if not username:
        legacy_id = _allocate_legacy_player_id(context)
        username = f"Player{legacy_id}"
    else:
        legacy_id = _allocate_legacy_player_id(context)
    username = _filter_user_text(
        context,
        username,
        policy="reject_profile",
        field_context="profile.username",
    )

    _assert_account_creation_allowed(
        context,
        request,
        [
            ("username_lower", username),
            ("account_id", f"steam:{platform_id}"),
            ("account_id", f"{API_VERSION}:platform:{platform}:{platform_id}"),
        ],
    )

    context.assert_username_available(username)

        # Platform identity groups profiles and is not a unique player identity.
    profile_identity = f"profile:{platform}:{platform_id}:{legacy_id}"
    player = context.get_or_create_player(
        API_VERSION,
        username=username,
        display_name=username,
        identity_key=profile_identity,
    )
    _initialize_new_account_avatar(player, context)

    _enforce_private_verified_account(player, context)
    state = {
        "legacy_player_id": legacy_id,
        "identity_key": profile_identity,
        "platform": platform,
        "platform_id": platform_id,
        "name": username,
        "bio": DEFAULT_PLAYER_BIO,
    }
    _initialize_25april2019_tutorial(player, state, context)

    token = f"local-{API_VERSION}-{legacy_id}"
    state["recnet_id"] = legacy_id

    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )

    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"steam:{platform_id}"),
            ("account_id", f"{API_VERSION}:platform:{platform}:{platform_id}"),
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )

    player["state"] = state
    return JSONResponse(await _make_login_response(player, token, context))


async def _handle_login_account_v2(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    username = _BASE._str_field(payload, "username", "Username", "usernameOrEmail", "UsernameOrEmail")
    
    player = None
    if username:
        player = _find_player_by_username(context, username)
        if player is None:
            player = _find_canonical_player_by_username(context, username)
            if player is not None:
                _bootstrap_25april2019_version_state(player, context)
        
    if player is None:
        return JSONResponse({
            "Player": None,
            "Token": None,
            "Error": "AccountNotFound",
            "FirstLoginOfTheDay": False,
            "AnalyticsSessionId": 0,
            "CachedPlatformMask": 0
        })
        
    legacy_id = int(player.get("state", {}).get("legacy_player_id") or 1)
    token = f"local-{API_VERSION}-{legacy_id}"
    
    state = player.get("state") or {}
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )
        
    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )
    
    player["state"] = state
    return JSONResponse(await _make_login_response(player, token, context))


async def _handle_login_cached_v2(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    profile_id = _BASE._int_field(payload, "ProfileId", "profileId", "PlayerId", "playerId", default=0)
    platform_id = _BASE._str_field(payload, "PlatformId", "platformId", "platform_id")
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)
    
    player = None
    if platform_id:
        if profile_id > 0:
    # Resolve the selected profile and verify platform ownership.
            selected = _find_player_by_legacy_id_25april2019(context, profile_id)
            selected_state = (selected or {}).get("state") or {}
            if (
                selected is not None
                and int(selected_state.get("platform", 0) or 0) == platform
                and str(selected_state.get("platform_id") or "") == platform_id
                and not bool(selected_state.get("cached_login_hidden", False))
                and not bool(selected_state.get("deleted", False))
            ):
                player = selected
        else:
            platform_player = _find_player_by_platform_25april2019(
                context, platform=platform, platform_id=platform_id
            )
            platform_state = (platform_player or {}).get("state") or {}
            if platform_player is not None and not bool(platform_state.get("deleted", False)):
                player = platform_player
        
    if player is None:
        return JSONResponse({
            "Player": None,
            "Token": None,
            "Error": "AccountNotFound",
            "FirstLoginOfTheDay": False,
            "AnalyticsSessionId": 0,
            "CachedPlatformMask": 0
        })
        
    legacy_id = int(player.get("state", {}).get("legacy_player_id") or 1)
    token = f"local-{API_VERSION}-{legacy_id}"
    
    state = player.get("state") or {}
    await _clear_active_game_session_for_login(player, state, context)
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
              AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )
        
    context.record_player_identities(
        player["player_id"],
        [
            ("account_id", f"recnet:{legacy_id}"),
            ("account_id", f"{API_VERSION}:recnet:{legacy_id}"),
        ],
    )
    
    player["state"] = state
    return JSONResponse(await _make_login_response(player, token, context))


async def _handle_local_profile(request: Request, context) -> Response:
    player_id = _local_profile_id(request, context)
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    
    profile_data = _serialize_profile_25april2019(player)
    return JSONResponse(profile_data)


async def _handle_profile_by_id(player_id: int, context) -> Response:
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    context.assert_player_not_banned(player["player_id"])
    return JSONResponse(_serialize_profile_25april2019(player))


async def _handle_profile_list(request: Request, context) -> Response:
    body = await request.body()
    decoded: Any = None
    if body:
        try:
            decoded = json.loads(body.decode("utf-8", errors="replace"))
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = None
    # Profiles.GetFromServer posts a bare JSON array, not an Ids object.
    if isinstance(decoded, list):
        raw_ids = decoded
    else:
        payload = await _BASE._parse_client_payload(request)
        raw_ids = payload.get(
            "Ids", payload.get("ids", payload.get("PlayerIds", payload.get("playerIds", [])))
        )
    if isinstance(raw_ids, str):
        raw_ids = [part for part in re.split(r"[,;\s]+", raw_ids) if part]
    if not isinstance(raw_ids, list):
        raw_ids = []
    profiles = []
    for raw_id in raw_ids[:100]:
        try:
            player_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        player = _find_player_by_legacy_id_25april2019(context, player_id)
        if player is not None:
            context.assert_player_not_banned(player["player_id"])
            profiles.append(_serialize_profile_25april2019(player))
    return JSONResponse(profiles)


async def _handle_profiles_by_platform_ids(request: Request, context) -> Response:
    payload = await _BASE._parse_client_payload(request)
    platform = _BASE._int_field(payload, "Platform", "platform", default=0)
    raw_ids = payload.get("PlatformIds", payload.get("platformIds", []))
    if isinstance(raw_ids, str):
        raw_ids = [part for part in re.split(r"[,;\s]+", raw_ids) if part]
    if not isinstance(raw_ids, list):
        raw_ids = []
    profiles = []
    for platform_id in raw_ids[:100]:
        player = _find_player_by_platform_25april2019(
            context,
            platform=platform,
            platform_id=str(platform_id),
        )
        if player is not None:
            context.assert_player_not_banned(player["player_id"])
            profiles.append(_serialize_profile_25april2019(player))
    return JSONResponse(profiles)


async def _handle_profile_name_lookup(request: Request, context, *, search: bool) -> Response:
    name = str(request.query_params.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required.")
    if not search:
        player = _find_player_by_username(context, name)
        if player is None:
            raise HTTPException(status_code=404, detail="Player not found.")
        context.assert_player_not_banned(player["player_id"])
        return JSONResponse(_serialize_profile_25april2019(player))

    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ?
              AND (lower(p.username) LIKE ? OR lower(p.display_name) LIKE ?)
            ORDER BY p.username
            LIMIT 20
            """,
            (API_VERSION, f"%{name.casefold()}%", f"%{name.casefold()}%"),
        ).fetchall()
    profiles = []
    for row in rows:
        player = {key: row[key] for key in row.keys() if key != "state_json"}
        try:
            player["state"] = json.loads(row["state_json"] or "{}")
        except Exception:
            player["state"] = {}
        profiles.append(_serialize_profile_25april2019(player))
    return JSONResponse(profiles)


async def _handle_config_v2(request: Request, context) -> Response:
    origin = context.public_api_origin(request, API_VERSION)
    
    level_progression = context.get_server_setting("level_progression_maps", LEVEL_PROGRESSION_MAPS)
    if not isinstance(level_progression, list) or not level_progression:
        level_progression = LEVEL_PROGRESSION_MAPS
    daily_objectives = context.get_server_setting("daily_objectives", DAILY_OBJECTIVES_DEFAULTS)
    if not isinstance(daily_objectives, list) or not daily_objectives:
        daily_objectives = DAILY_OBJECTIVES_DEFAULTS
    maintenance_state = context.get_maintenance_state()
    maintenance_payload = (
        {"StartsInMinutes": int(maintenance_state["starts_in_minutes"])}
        if maintenance_state["active"]
        else None
    )

    return JSONResponse({
        "CdnBaseUri": f"{origin}/{API_VERSION}/",
        "LevelProgressionMaps": level_progression,
        "DailyObjectives": daily_objectives,
        "ServerMaintenance": maintenance_payload,
        "AutoMicMutingConfig": {
            "MicSpamVolumeThreshold": 0.8,
            "MicVolumeSampleInterval": 0.1,
            "MicVolumeSampleRollingWindowLength": 5.0,
            "MicSpamSamplePercentageForWarning": 0.5,
            "MicSpamSamplePercentageForWarningToEnd": 0.2,
            "MicSpamSamplePercentageForForceMute": 0.8,
            "MicSpamSamplePercentageForForceMuteToEnd": 0.4,
            "MicSpamWarningStateVolumeMultiplier": 0.5
        }
    })


async def _handle_gameconfigs_all(request: Request, context) -> Response:
    configured = context.get_server_setting("game_configs", {})
    if not isinstance(configured, dict):
        configured = {}
    values = {
        # This client compares the heartbeat flag to the string "1".
        "UseHeartbeatWebSocket": "1",
        # SplitTesting.ParseAndAssign removes empty entries, so an empty value
        # is the exact no-override representation accepted by this client.
        "splitTestSoftOverrides": "",
        "splitTestHardOverrides": "",
        # BootSequence parses this through Single.TryParse before applying it.
        "GASamplingRatio": "0",
        # DoorID keys control room searches and return spawn matching.
        "Door.Shooters.Query": "#pvp",
        "Door.Shooters.Title": "PVP",
        "Door.Creative.Query": "#puzzle | #art",
        "Door.Creative.Title": "PUZZLE",
        "Door.Quests.Query": "#quest",
        "Door.Quests.Title": "QUESTS",
        "Door.Sports.Query": "#sport",
        "Door.Sports.Title": "SPORTS & REC",
        "Door.Featured.Query": "#featured -dormroom",
        "Door.Featured.Title": "Featured",
    }
    values.update({str(key): str(value) for key, value in configured.items()})
    return JSONResponse(
        [
            {"Key": key, "Value": value, "StartTime": None, "EndTime": None}
            for key, value in sorted(values.items())
        ]
    )


async def _handle_presence_heartbeat(request: Request, context) -> Response:
    payload = dict(request.query_params)
    payload.update(await _BASE._parse_client_payload(request))
    login_lock_token = _BASE._str_field(payload, "LoginLockToken", "loginLockToken")
    requested_session_id = _BASE._int_field(
        payload, "GameSessionId", "gameSessionId", default=0
    )
    player = context.player_from_request(request, API_VERSION)
    if player is not None and login_lock_token:
        await _remember_presence_login_lock_token(player, login_lock_token, context)
    if player is None and login_lock_token:
        player = await _player_from_presence_login_lock_token(context, login_lock_token)
    if player is None:
        raise HTTPException(status_code=401, detail="Authenticated player is required.")

    try:
        state = json.loads(player["state_json"] or "{}")
    except Exception:
        state = {}
    legacy_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
    if legacy_id <= 0:
        raise HTTPException(status_code=409, detail="Player has no 2019 profile ID.")
    await _mark_presence_heartbeat(legacy_id, context, online=True)
    error, presence = await _presence_for_heartbeat(
        legacy_id, requested_session_id, context
    )
    return JSONResponse({"Error": error, "Presence": presence})


async def _authoritative_game_session_for_player(
    legacy_id: int, state: dict[str, Any], context
) -> dict[str, Any] | None:
    """Resolve the current game session from the Redis membership lease."""
    if legacy_id <= 0:
        return None
    membership = await context.require_transient().get_membership(legacy_id)
    return membership if isinstance(membership, dict) else None


async def _remember_presence_login_lock_token(player, token: str, context) -> None:
    token = str(token or "").strip()
    if not token or len(token) > 200:
        return
    await context.require_transient().remember_token_lookup(
        "25april2019-presence-login-lock",
        token,
        _legacy_id_for_player(player),
        ttl_seconds=15 * 60,
    )


async def _player_from_presence_login_lock_token(context, token: str) -> dict[str, Any] | None:
    token = str(token or "").strip()
    if not token or len(token) > 200:
        return None
    if token.startswith(f"local-{API_VERSION}-"):
        if await context.player_session_for_token(token, API_VERSION) is None:
            return None
        try:
            legacy_id = int(token.rsplit("-", 1)[1])
        except (TypeError, ValueError):
            legacy_id = 0
        player = (
            _find_player_by_legacy_id_25april2019(context, legacy_id)
            if legacy_id > 0
            else None
        )
        if player is not None:
            context.assert_player_not_banned(player["player_id"])
        return player
    value = await context.require_transient().token_lookup(
        "25april2019-presence-login-lock", token
    )
    try:
        legacy_id = int(value or 0)
    except (TypeError, ValueError):
        legacy_id = 0
    player = (
        _find_player_by_legacy_id_25april2019(context, legacy_id)
        if legacy_id > 0
        else None
    )
    if player is not None:
        context.assert_player_not_banned(player["player_id"])
    return player


async def _presence_payload(
    legacy_id: int, state: dict[str, Any], context=None
) -> dict[str, Any]:
    is_online = bool(
        context is not None
        and await context.require_transient().player_online(legacy_id)
    )
    game_session = None
    if context is not None:
        membership = await context.require_transient().get_membership(legacy_id)
        if isinstance(membership, dict):
            game_session = membership
        elif is_online:
            # Resolve authoritative membership from transient state.
            game_session = await _authoritative_game_session_for_player(
                legacy_id, state, context
            )
    return {
        "PlayerId": legacy_id,
        "IsOnline": is_online,
        "PlayerType": int(state.get("player_type") or 0),
        "StatusVisibility": int(state.get("status_visibility") or 0),
        "GameSession": game_session,
    }


async def _handle_presence_list(request: Request, context) -> Response:
    _authenticated_player(request, context)
    try:
        payload = json.loads((await request.body()).decode("utf-8", errors="replace"))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Presence request must be a JSON player-id list.") from exc
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Presence request must be a JSON player-id list.")
    player_ids = []
    for value in payload:
        try:
            player_id = int(value)
        except (TypeError, ValueError):
            continue
        if player_id > 0 and player_id not in player_ids:
            player_ids.append(player_id)
    presences: list[dict[str, Any]] = []
    for player_id in player_ids:
        player = _find_player_by_legacy_id_25april2019(context, player_id)
        if player is None:
            continue
        presences.append(await _presence_payload(player_id, _player_state(player), context))
    return JSONResponse(presences)


async def _handle_presence_state(request: Request, context, field_name: str, *payload_names: str) -> Response:
    row = context.player_from_request(request, API_VERSION)
    if row is None:
        raise HTTPException(status_code=401, detail="Authenticated player is required.")
    payload = await _BASE._parse_client_payload(request)
    value = _BASE._int_field(payload, *payload_names, default=0)
    _patch_player_state(row, context, set_values={field_name: value})
    _schedule_presence_update(_legacy_id_for_player(row), context)
    return Response(status_code=204)


async def _handle_presence_disconnected(request: Request, context) -> Response:
    reporter = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    remote_player_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    game_session_id = _BASE._int_field(
        payload, "GameSessionId", "gameSessionId", default=0
    )
    if remote_player_id <= 0 or game_session_id <= 0:
        raise HTTPException(
            status_code=400,
            detail="PlayerId and GameSessionId are required.",
        )

    # This reports a remote peer leaving; it does not log out the reporter.
    reporter_id = _legacy_id_for_player(reporter)
    members = {
        int(value)
        for value in await context.require_transient().session_member_ids(
            game_session_id
        )
        if str(value).lstrip("-").isdigit()
    }
    # Ignore stale reports unless both players still share this instance.
    removed = bool(
        reporter_id in members
        and remote_player_id in members
        and await context.require_transient().remove_session_member(
            game_session_id, remote_player_id
        )
    )

    if removed:
        remote_player = _find_player_by_legacy_id_25april2019(
            context, remote_player_id
        )
        if remote_player is not None:
            _schedule_presence_update(remote_player_id, context)
    return Response(status_code=204)


async def _handle_logout(request: Request, context) -> Response:
    row = context.player_from_request(request, API_VERSION)
    if row is None:
        return Response(status_code=204)
    aliases = context.transient_player_aliases(row["player_id"])
    for alias in aliases:
        await context.require_transient().delete_secret(
            f"{API_VERSION}-hub", alias
        )
    await context.revoke_request_player_session(request)
    await context.require_transient().revoke_player_transient_state(
        row["player_id"],
        aliases=aliases,
    )
    return Response(status_code=204)


def _normalize_player_settings(settings: Any) -> dict[str, str]:
    normalized = (
        {str(key): str(value) for key, value in settings.items()}
        if isinstance(settings, dict)
        else {}
    )
    for key, value in DEFAULT_PLAYER_SETTINGS.items():
        normalized.setdefault(key, value)
    if normalized["DeveloperDisplayMode"] not in VALID_DEVELOPER_DISPLAY_MODES:
        normalized["DeveloperDisplayMode"] = "0"
    return normalized


def _tutorial_preferences_for_player(
    player, context, shared_settings: dict[str, str]
) -> dict[str, str] | None:
    stored = _BASE._get_json_setting(
        context,
        _tutorial_player_setting_key(str(player["player_id"])),
        {},
    )
    if not isinstance(stored, dict) or not stored:
        return None
    return {
        key: str(stored.get(key, shared_settings.get(key, default)))
        for key, default in TUTORIAL_PREFERENCE_DEFAULTS.items()
    }


async def _handle_get_settings_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("player_settings", player["player_id"])
    stored_settings = _BASE._get_json_setting(context, key, {})
    settings = _normalize_player_settings(stored_settings)
    if settings != stored_settings:
        _BASE._set_json_setting(context, key, settings)
    tutorial_preferences = _tutorial_preferences_for_player(player, context, settings)
    if tutorial_preferences is not None:
        settings.update(tutorial_preferences)
    return JSONResponse([{"Key": str(k), "Value": str(v)} for k, v in settings.items()])


async def _handle_set_setting_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    key_name = _BASE._str_field(payload, "Key", "key")
    if not key_name:
        raise HTTPException(status_code=400, detail="Key is required.")
    key = _canonical_player_setting_key("player_settings", player["player_id"])
    settings = _normalize_player_settings(_BASE._get_json_setting(context, key, {}))
    value = _BASE._str_field(payload, "Value", "value")
    if key_name == "DeveloperDisplayMode" and value not in VALID_DEVELOPER_DISPLAY_MODES:
        value = "0"
    if key_name in TUTORIAL_PREFERENCE_KEYS:
        tutorial_preferences = _tutorial_preferences_for_player(player, context, settings)
        if tutorial_preferences is None:
            tutorial_preferences = {
                tutorial_key: str(settings.get(tutorial_key, default))
                for tutorial_key, default in TUTORIAL_PREFERENCE_DEFAULTS.items()
            }
        tutorial_preferences[key_name] = value
        _BASE._set_json_setting(
            context,
            _tutorial_player_setting_key(str(player["player_id"])),
            tutorial_preferences,
        )
        return Response(status_code=204)
    settings[key_name] = value
    _BASE._set_json_setting(context, key, settings)
    return Response(status_code=204)


async def _handle_remove_setting_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    key_name = _BASE._str_field(payload, "Key", "key")
    if not key_name:
        raise HTTPException(status_code=400, detail="Key is required.")
    key = _canonical_player_setting_key("player_settings", player["player_id"])
    settings = _normalize_player_settings(_BASE._get_json_setting(context, key, {}))
    if key_name in TUTORIAL_PREFERENCE_KEYS:
        tutorial_preferences = _tutorial_preferences_for_player(player, context, settings)
        if tutorial_preferences is not None:
            tutorial_preferences.pop(key_name, None)
            _BASE._set_json_setting(
                context,
                _tutorial_player_setting_key(str(player["player_id"])),
                tutorial_preferences,
            )
        return Response(status_code=204)
    if key_name in settings:
        del settings[key_name]
    settings = _normalize_player_settings(settings)
    _BASE._set_json_setting(context, key, settings)
    return Response(status_code=204)


async def _handle_get_avatar_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("player_avatar", player["player_id"])
    avatar = _BASE._get_json_setting(context, key, {})
    avatar = _normalize_avatar(avatar)
    if avatar != _BASE._get_json_setting(context, key, {}):
        _BASE._set_json_setting(context, key, avatar)
    return JSONResponse(avatar)


def _normalize_avatar(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        payload = {}
    outfit = str(payload.get("OutfitSelections") or "").strip()
    records = [record for record in outfit.split(";") if record]
    if not records or any(len(record.split(",")) != 5 for record in records):
        outfit = DEFAULT_AVATAR["OutfitSelections"]

    face_features = str(payload.get("FaceFeatures") or "").strip()
    try:
        face_payload = json.loads(face_features) if face_features else None
    except Exception:
        face_payload = None
    if not isinstance(face_payload, dict) or not face_payload.get("eyeId") or not face_payload.get("mouthId"):
        face_features = DEFAULT_AVATAR["FaceFeatures"]

    skin_color = str(payload.get("SkinColor") or "").strip()
    if not skin_color or skin_color.startswith("#"):
        skin_color = DEFAULT_AVATAR["SkinColor"]
    hair_color = str(payload.get("HairColor") or "").strip()
    if not hair_color or hair_color.startswith("#"):
        hair_color = DEFAULT_AVATAR["HairColor"]
    return {
        "OutfitSelections": outfit,
        "FaceFeatures": face_features,
        "SkinColor": skin_color,
        "HairColor": hair_color,
    }


async def _handle_set_avatar_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    key = _canonical_player_setting_key("player_avatar", player["player_id"])
    avatar = _normalize_avatar(payload)
    _BASE._set_json_setting(context, key, avatar)
    return Response(status_code=204)


async def _handle_get_saved_outfits_v3(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("saved_outfits", player["player_id"])
    outfits = _BASE._get_json_setting(context, key, {})
    if not isinstance(outfits, dict):
        outfits = {}
    ordered = [value for _, value in sorted(outfits.items()) if isinstance(value, dict)]
    return JSONResponse(ordered)


async def _handle_set_saved_outfit_v3(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    slot = _BASE._int_field(payload, "Slot", "slot", default=-1)
    if slot < 0:
        raise HTTPException(status_code=400, detail="Slot is required.")
    outfit = {
        "Slot": slot,
        "PreviewImageName": _BASE._str_field(payload, "PreviewImageName", "previewImageName"),
        **_normalize_avatar(payload),
    }
    key = _canonical_player_setting_key("saved_outfits", player["player_id"])
    outfits = _BASE._get_json_setting(context, key, {})
    if not isinstance(outfits, dict):
        outfits = {}
    outfits[str(slot)] = outfit
    _BASE._set_json_setting(context, key, outfits)
    return Response(status_code=204)


async def _handle_get_unlocked_items_v3(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT item_key, state_json
            FROM inventory_items
            WHERE player_id = ? AND quantity > 0
            ORDER BY item_key
            """,
            (player["player_id"],),
        ).fetchall()
    # Only default selections and persisted purchases are entitlements.
    items_by_key = {
        (0, item_desc): {
            "AvatarItemType": 0,
            "AvatarItemDesc": item_desc,
            "UnlockedLevel": 0,
            "PlatformMask": -1,
        }
        for item_desc in dict.fromkeys(DEFAULT_UNLOCKED_AVATAR_ITEM_DESCS)
    }
    for row in rows:
        try:
            state = json.loads(row["state_json"] or "{}")
        except Exception:
            state = {}
        if not isinstance(state, dict):
            continue
        item_type = state.get("AvatarItemType", state.get("avatar_item_type"))
        if item_type is None:
            continue
        item_desc = _normalize_avatar_item_desc(
            str(state.get("AvatarItemDesc") or state.get("avatar_item_desc") or row["item_key"])
        )
        network_item_type = int(item_type)
        if network_item_type not in {0, 1}:
            network_item_type = 0
        items_by_key[(network_item_type, item_desc)] = {
            "AvatarItemType": network_item_type,
            "AvatarItemDesc": item_desc,
            "UnlockedLevel": int(state.get("UnlockedLevel", state.get("unlocked_level", 0)) or 0),
            "PlatformMask": int(state.get("PlatformMask", state.get("platform_mask", -1)) or -1),
        }
    return JSONResponse(list(items_by_key.values()))


async def _handle_get_gifts_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT gift_box_id, state_json
            FROM gift_boxes
            WHERE player_id = ? AND opened = 0
            ORDER BY created_at
            """,
            (player["player_id"],),
        ).fetchall()
    gifts = []
    for row in rows:
        try:
            state = json.loads(row["state_json"] or "{}")
        except Exception:
            state = {}
        if not isinstance(state, dict) or "Id" not in state:
            continue
        # Normalize stored values to GiftRarity's sparse enum.
        raw_rarity = int(state.get("GiftRarity", 0) or 0)
        if raw_rarity not in {-1, 0, 10, 20, 30, 50}:
            state["GiftRarity"] = {
                1: 0,
                2: 10,
                3: 20,
                4: 30,
                5: 50,
                40: 50,
            }.get(raw_rarity, 0)
        gifts.append(state)
    return JSONResponse(gifts)


def _player_owns_avatar_item(player: dict[str, Any], item_desc: str, context) -> bool:
    normalized = _normalize_avatar_item_desc(item_desc)
    if normalized in {_normalize_avatar_item_desc(value) for value in DEFAULT_UNLOCKED_AVATAR_ITEM_DESCS}:
        return True
    with context.db.connection() as conn:
        rows = conn.execute(
            "SELECT state_json FROM inventory_items WHERE player_id = ? AND quantity > 0",
            (player["player_id"],),
        ).fetchall()
    for row in rows:
        try:
            state = json.loads(row["state_json"] or "{}")
        except Exception:
            continue
        if not isinstance(state, dict):
            continue
        owned_desc = state.get("AvatarItemDesc", state.get("avatar_item_desc"))
        if owned_desc and _normalize_avatar_item_desc(str(owned_desc)) == normalized:
            return True
    return False


def _quest_alternate_context(primary_context: int) -> int | None:
    if 4000 <= primary_context <= 4003:
        return 4004
    if 4010 <= primary_context <= 4013:
        return 4014
    if 4100 <= primary_context <= 4104:
        return 4105
    if primary_context in {4200, 4201, 4202, 4203, 4204, 4206}:
        return 4205
    if 4500 <= primary_context <= 4503:
        return 4504
    return None


async def _handle_generate_gift_v2(request: Request, context) -> Response:
    """Generate and persist a bare GiftPackage for one-time consumption."""
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    primary_context = _gift_context_value(payload.get("GiftContext", payload.get("giftContext", 0)))
    alternate_value = payload.get("AlternateGiftContext", payload.get("alternateGiftContext"))
    alternate_context = _gift_context_value(alternate_value) if alternate_value is not None else None
    state = _player_state(player)

    # Prefer the primary rank context; the alternate is consumable-only.
    gift_context = primary_context
    friendly_name = "Rec Room reward"
    consumable_desc: str | None = None
    avatar_item_desc: str | None = None
    currency_type = 2
    # Keep activity awards stable per player, context, and UTC day.
    currency = _large_token_award(
        "gift-currency",
        _legacy_id_for_player(player),
        primary_context,
        datetime.now(timezone.utc).date().isoformat(),
    )
    gift_level = 0
    gift_rarity = 0

    rank_reward = QUEST_RANK_REWARDS.get(primary_context)
    if rank_reward is not None and not _player_owns_avatar_item(player, rank_reward[1], context):
        friendly_name, raw_avatar_item_desc, gift_rarity = rank_reward
        avatar_item_desc = _normalize_avatar_item_desc(raw_avatar_item_desc)
        currency_type = 0
        currency = 0
    else:
        if rank_reward is not None and alternate_context not in QUEST_CONSUMABLE_REWARDS:
            alternate_context = _quest_alternate_context(primary_context)
        consumable_reward = QUEST_CONSUMABLE_REWARDS.get(
            alternate_context if alternate_context is not None else primary_context
        )
        if consumable_reward is not None:
            gift_context = alternate_context if alternate_context is not None else primary_context
            friendly_name, consumable_desc = consumable_reward
            currency_type = 0
            currency = 0
        elif primary_context == 100 or 102 <= primary_context <= 130:
            gift_level = max(
                1,
                min(
                    MAX_PLAYER_LEVEL,
                    primary_context - 100 if primary_context >= 102 else int(player["canonical_level"] or 1),
                ),
            )
            friendly_name = f"Level {gift_level} reward"
            currency = _large_token_award("level-reward", gift_level)

    requested_message = str(payload.get("Message", payload.get("message", "")) or "").strip()
    gift_id = secrets.randbelow(8_000_000_000_000_000) + 1_000_000_000_000_000
    gift_package = {
        "Id": gift_id,
        "FromPlayerId": None,
        "ConsumableItemDesc": consumable_desc,
        "AvatarItemType": 0 if avatar_item_desc else None,
        "AvatarItemDesc": avatar_item_desc,
        "EquipmentPrefabName": None,
        "EquipmentModificationGuid": None,
        "CurrencyType": currency_type,
        "Currency": currency,
        "Xp": 0,
        "Level": gift_level,
        "GiftContext": gift_context,
        "GiftRarity": gift_rarity,
        "Message": requested_message or friendly_name,
        "Platform": int(state.get("platform", 0) or 0),
        "Consumed": False,
        "IsValid": True,
        "ErrorMessage": "",
        "SupportsCurrentPlatform": True,
        # Extra server-only state is ignored by GiftPackage.Deserialize.
        "ServerGrantApplied": False,
        "RequestedGiftContext": primary_context,
        "IsGameGift": _bool_value(payload.get("IsGameGift", payload.get("isGameGift", False))),
    }
    with _PLAYER_STATE_LOCK:
        with context.db.transaction() as conn:
            existing_rows = conn.execute(
                "SELECT state_json FROM gift_boxes WHERE player_id = ? AND opened = 0 ORDER BY created_at",
                (player["player_id"],),
            ).fetchall()
            for existing_row in existing_rows:
                try:
                    existing_state = json.loads(existing_row["state_json"] or "{}")
                except Exception:
                    existing_state = {}
                if (
                    isinstance(existing_state, dict)
                    and int(existing_state.get("RequestedGiftContext", -1) or -1) == primary_context
                ):
                    return JSONResponse(existing_state)
            conn.execute(
            """
            INSERT INTO gift_boxes(
                gift_box_id, player_id, state_json, opened, created_at, updated_at
            )
            VALUES (?, ?, ?, 0,
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            """,
                (str(gift_id), player["player_id"], json.dumps(gift_package, sort_keys=True)),
            )
    return JSONResponse(gift_package)


async def _handle_consume_gift_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    gift_id = _BASE._int_field(payload, "Id", "id", default=0)
    if gift_id <= 0:
        raise HTTPException(status_code=400, detail="Id is required.")
    unlocked_level = max(0, _BASE._int_field(payload, "UnlockedLevel", "unlockedLevel", default=0))
    balance_update: dict[str, int] | None = None
    profile_changed = False
    with _PLAYER_STATE_LOCK:
        with context.db.transaction() as conn:
            gift_row = conn.execute(
                """
                SELECT state_json, opened
                FROM gift_boxes
                WHERE player_id = ? AND gift_box_id = ?
                """,
                (player["player_id"], str(gift_id)),
            ).fetchone()
            if gift_row is None:
                # Consuming an already-cleaned gift is idempotent.
                return Response(status_code=204)
            try:
                gift_state = json.loads(gift_row["state_json"] or "{}")
            except Exception:
                gift_state = {}
            if not isinstance(gift_state, dict):
                gift_state = {}
            grant_applied = bool(gift_state.get("ServerGrantApplied", False))
            currency_type = int(gift_state.get("CurrencyType", 0) or 0)
            currency = max(0, int(gift_state.get("Currency", 0) or 0))
            if not grant_applied:
            # Gameplay state is version-owned; players stores identity only.
                player_row = conn.execute(
                    """
                    SELECT state_json
                    FROM player_version_state
                    WHERE player_id = ? AND api_version = ?
                    """,
                    (player["player_id"], API_VERSION),
                ).fetchone()
                try:
                    player_state = json.loads(player_row["state_json"] or "{}") if player_row else {}
                except Exception:
                    player_state = {}
                if not isinstance(player_state, dict):
                    player_state = {}
                player_state_changed = False
                if currency > 0 and currency_type in {1, 2, 100, 101, 200}:
                    balances = player_state.get("storefront_balances")
                    if not isinstance(balances, dict):
                        balances = {}
                    current_balance = max(
                        0,
                        int(
                            balances.get(
                                str(currency_type),
                                TOKEN_BALANCE if currency_type == 2 else 0,
                            )
                            or 0
                        ),
                    ) + currency
                    balances[str(currency_type)] = current_balance
                    player_state["storefront_balances"] = balances
                    player_state_changed = True
                    balance_update = {
                        "Balance": current_balance,
                        "CurrencyType": currency_type,
                        "Platform": int(player_state.get("platform", 0) or 0),
                    }

                avatar_item_desc = gift_state.get("AvatarItemDesc")
                if avatar_item_desc:
                    normalized_desc = _normalize_avatar_item_desc(str(avatar_item_desc))
                    conn.execute(
                        """
                        INSERT INTO inventory_items(player_id, item_key, quantity, state_json, created_at, updated_at)
                        VALUES (?, ?, 1, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                        ON CONFLICT(player_id, item_key) DO UPDATE SET
                            quantity = 1, state_json = excluded.state_json,
                            updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                        """,
                        (
                            player["player_id"],
                            f"{API_VERSION}:avatar:{normalized_desc}",
                            json.dumps(
                                {
                                    "AvatarItemType": int(gift_state.get("AvatarItemType", 0) or 0),
                                    "AvatarItemDesc": normalized_desc,
                                    "UnlockedLevel": unlocked_level,
                                    "PlatformMask": -1,
                                },
                                sort_keys=True,
                            ),
                        ),
                    )

                equipment_prefab = gift_state.get("EquipmentPrefabName")
                equipment_guid = gift_state.get("EquipmentModificationGuid")
                if equipment_prefab and equipment_guid:
                    conn.execute(
                        """
                        INSERT INTO inventory_items(player_id, item_key, quantity, state_json, created_at, updated_at)
                        VALUES (?, ?, 1, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                        ON CONFLICT(player_id, item_key) DO UPDATE SET
                            quantity = 1, state_json = excluded.state_json,
                            updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                        """,
                        (
                            player["player_id"],
                            f"{API_VERSION}:equipment:{equipment_prefab}:{equipment_guid}",
                            json.dumps(
                                {
                                    "PrefabName": str(equipment_prefab),
                                    "ModificationGuid": str(equipment_guid),
                                    "UnlockedLevel": unlocked_level,
                                    "PlatformMask": -1,
                                },
                                sort_keys=True,
                            ),
                        ),
                    )

                consumable_desc = gift_state.get("ConsumableItemDesc")
                if consumable_desc:
                    category = next(
                        (value for _, desc, value in BUILD_CONSUMABLES if desc == consumable_desc),
                        0,
                    )
                    limit_count, limit_type = BUILD_CONSUMABLE_LIMITS.get(category, (1, 0))
                    item_key = f"{API_VERSION}:consumable:{consumable_desc}"
                    inventory_row = conn.execute(
                        "SELECT quantity, state_json FROM inventory_items WHERE player_id = ? AND item_key = ?",
                        (player["player_id"], item_key),
                    ).fetchone()
                    existing_quantity = int(inventory_row["quantity"] or 0) if inventory_row else 0
                    try:
                        consumable_state = json.loads(inventory_row["state_json"] or "{}") if inventory_row else {}
                    except Exception:
                        consumable_state = {}
                    if not isinstance(consumable_state, dict):
                        consumable_state = {}
                    existing_quantity, consumable_state = _settle_realtime_consumable(
                        consumable_state, existing_quantity
                    )
                    quantity = existing_quantity + limit_count
                    consumable_state.update(
                        {
                            "Id": gift_id,
                            "ConsumableItemDesc": str(consumable_desc),
                            "Category": category,
                            "PlatformMask": -1,
                            "InitialCount": quantity,
                            "LimitCount": limit_count,
                            "LimitType": limit_type,
                            "UnlockedLevel": unlocked_level,
                            "IsActive": _bool_value(consumable_state.get("IsActive")),
                        }
                    )
                    conn.execute(
                        """
                        INSERT INTO inventory_items(player_id, item_key, quantity, state_json, created_at, updated_at)
                        VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                        ON CONFLICT(player_id, item_key) DO UPDATE SET
                            quantity = excluded.quantity, state_json = excluded.state_json,
                            updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                        """,
                        (player["player_id"], item_key, quantity, json.dumps(consumable_state, sort_keys=True)),
                    )

                xp_award = max(0, int(gift_state.get("Xp", 0) or 0))
                if xp_award:
                    player_row = conn.execute(
                        "SELECT canonical_level, canonical_xp FROM players WHERE player_id = ?",
                        (player["player_id"],),
                    ).fetchone()
                    if player_row is not None:
                        level, within_level_xp = _player_level_progress(
                            _total_player_xp(player_row["canonical_level"], player_row["canonical_xp"]) + xp_award
                        )
                        conn.execute(
                            "UPDATE players SET canonical_level = ?, canonical_xp = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE player_id = ? AND is_coach = 0",
                            (level, within_level_xp, player["player_id"]),
                        )
                        profile_changed = True

                if player_state_changed:
                    conn.execute(
                        """
                        UPDATE player_version_state
                        SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                        WHERE player_id = ? AND api_version = ?
                        """,
                        (json.dumps(player_state, sort_keys=True), player["player_id"], API_VERSION),
                    )
            gift_state["ServerGrantApplied"] = True
            gift_state["Consumed"] = True
            conn.execute(
                """
                UPDATE gift_boxes
                SET opened = 1, state_json = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE player_id = ? AND gift_box_id = ?
                """,
                (json.dumps(gift_state, sort_keys=True), player["player_id"], str(gift_id)),
            )
    _remove_pending_hub_notifications(
        _legacy_id_for_player(player),
        context,
        notification_ids={30, 31},
        message_id=gift_id,
    )
    if balance_update is not None:
        await _send_hub_notification(
            _legacy_id_for_player(player), 61, balance_update, context=context
        )
    if profile_changed:
        await _broadcast_profile_update(_legacy_id_for_player(player), context)
    # Purchased storefront contents are granted transactionally by buyItem.
    # This follow-up call only acknowledges that the client opened its package.
    return Response(status_code=204)


async def _handle_leaderboard_v1(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    objective_type = _BASE._int_field(
        payload, "ObjectiveType", "objectiveType", default=-1
    )
    sort_ascending = _bool_value(
        payload.get("SortAscending", payload.get("sortAscending", False))
    )
    requested_limit = _BASE._int_field(payload, "Limit", "limit", default=100)
    limit = max(1, min(requested_limit if requested_limit > 0 else 100, 100))

    leaderboard_period = _weekly_period(context)
    period_content = leaderboard_period["content"]
    period_key = (
        f"{int(period_content['iso_year']):04d}-"
        f"W{int(period_content['iso_week']):02d}"
    )
    next_reset = datetime.fromisoformat(
        str(leaderboard_period["ends_at_utc"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)

    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT p.player_id, p.canonical_level, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ? AND p.is_coach = 0
            """,
            (API_VERSION,),
        ).fetchall()

    scores: list[tuple[int, int, int]] = []
    for row in rows:
        try:
            state = json.loads(row["state_json"] or "{}")
        except Exception:
            state = {}
        if not isinstance(state, dict):
            state = {}
        legacy_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
        if legacy_id <= 0:
            continue
        totals = state.get("objective_completion_counts")
        if not isinstance(totals, dict):
            totals = {}
        periodic_all = state.get("objective_completion_periodic")
        if not isinstance(periodic_all, dict):
            periodic_all = {}
        periodic = periodic_all.get(period_key)
        if not isinstance(periodic, dict):
            periodic = {}
        if objective_type < 0:
            overall_count = sum(max(0, int(value or 0)) for value in totals.values())
            periodic_count = sum(max(0, int(value or 0)) for value in periodic.values())
        elif objective_type == 5 and str(objective_type) not in totals:
        # Use canonical level for accounts without objective counters.
            overall_count = max(0, int(row["canonical_level"] or 0))
            periodic_count = max(0, int(periodic.get(str(objective_type), 0) or 0))
        else:
            overall_count = max(0, int(totals.get(str(objective_type), 0) or 0))
            periodic_count = max(0, int(periodic.get(str(objective_type), 0) or 0))
        scores.append((legacy_id, overall_count, periodic_count))

    def ranked(index: int, allowed_ids: set[int] | None = None) -> list[dict[str, int]]:
        selected = [item for item in scores if allowed_ids is None or item[0] in allowed_ids]
        selected.sort(key=lambda item: (item[index], item[0]), reverse=not sort_ascending)
        return [
            {"PlayerId": item[0], "Count": item[index], "Order": order}
            for order, item in enumerate(selected[:limit], start=1)
        ]

    local_id = _legacy_id_for_player(player)
    friend_ids = {local_id}
    friend_ids.update(
        int(item.get("PlayerID") or 0)
        for item in _load_relationships(player, context)
        if int(item.get("RelationshipType") or 0) == 3
    )
    return JSONResponse({
        "GlobalOverall": ranked(1),
        "GlobalPeriodic": ranked(2),
        "FriendsOverall": ranked(1, friend_ids),
        "FriendsPeriodic": ranked(2, friend_ids),
        "NextResetUTC": next_reset.isoformat().replace("+00:00", "Z"),
    })


def _relationship_dto(item: dict[str, Any]) -> dict[str, int]:
    return {
        "PlayerID": int(item.get("PlayerID") or 0),
        "RelationshipType": int(item.get("RelationshipType") or 0),
        # These are ReciprocalStatus enum values (None/Local/Remote/Mutual),
        # not JSON booleans in the April 2019 Relationship DTO.
        "Muted": int(item.get("Muted") or 0),
        "Ignored": int(item.get("Ignored") or 0),
        "Favorited": int(item.get("Favorited") or 0),
    }


def _load_relationships(player, context) -> list[dict[str, int]]:
    key = _canonical_player_setting_key("relationships", player["player_id"])
    raw = _BASE._get_json_setting(context, key, [])
    if not isinstance(raw, list):
        return []
    return [
        _relationship_dto(item)
        for item in raw
        if isinstance(item, dict) and int(item.get("PlayerID") or 0) > 0
    ]


def _save_relationships(player, relationships: list[dict[str, int]], context) -> None:
    key = _canonical_player_setting_key("relationships", player["player_id"])
    _BASE._set_json_setting(
        context,
        key,
        sorted((_relationship_dto(item) for item in relationships), key=lambda item: item["PlayerID"]),
    )


def _relationship_entry(relationships: list[dict[str, int]], target_id: int) -> dict[str, int]:
    for item in relationships:
        if int(item.get("PlayerID") or 0) == target_id:
            return item
    item = {
        "PlayerID": target_id,
        "RelationshipType": 0,
        "Muted": 0,
        "Ignored": 0,
        "Favorited": 0,
    }
    relationships.append(item)
    return item


def _set_reciprocal_status(
    local: dict[str, int], remote: dict[str, int], field: str, enabled: bool
) -> None:
    remote_enabled = int(remote.get(field) or 0) in {1, 3}
    local[field] = 3 if enabled and remote_enabled else (1 if enabled else (2 if remote_enabled else 0))
    remote[field] = 3 if enabled and remote_enabled else (2 if enabled else (1 if remote_enabled else 0))


def _players_ignore_each_other(context, first_id: int, second_id: int) -> bool:
    if first_id <= 0 or second_id <= 0 or first_id == second_id:
        return False
    first = _find_player_by_legacy_id_25april2019(context, first_id)
    second = _find_player_by_legacy_id_25april2019(context, second_id)
    if first is None or second is None:
        return False
    first_relationship = next(
        (
            item
            for item in _load_relationships(first, context)
            if int(item.get("PlayerID") or 0) == second_id
        ),
        None,
    )
    second_relationship = next(
        (
            item
            for item in _load_relationships(second, context)
            if int(item.get("PlayerID") or 0) == first_id
        ),
        None,
    )
    return bool(
        (first_relationship and int(first_relationship.get("Ignored") or 0) != 0)
        or (second_relationship and int(second_relationship.get("Ignored") or 0) != 0)
    )


async def _notify_relationship(player_id: int, relationship: dict[str, int], context) -> None:
    await _send_hub_notification(player_id, 1, _relationship_dto(relationship), context=context)


async def _create_recnet_message(
    target,
    *,
    from_player_id: int,
    message_type: int,
    data: str = "",
    room_id: int = 0,
    context,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    message = {
        "Id": int(now.timestamp() * 1_000_000) * 1000 + secrets.randbelow(1000),
        "FromPlayerId": max(0, int(from_player_id)),
        "SentTime": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "Type": int(message_type),
        "Data": str(data),
        "RoomId": int(room_id) if int(room_id) > 0 else None,
        "PlayerEventId": None,
    }
    message_key = _canonical_player_setting_key("messages", target["player_id"])
    messages = _BASE._get_json_setting(context, message_key, [])
    if not isinstance(messages, list):
        messages = []
    messages.append(message)
    _BASE._set_json_setting(context, message_key, messages[-100:])
    await _send_hub_notification(
        _legacy_id_for_player(target), 2, message, context=context
    )
    return message


def _messages_for_player(player, context) -> list[dict[str, Any]]:
    key = _canonical_player_setting_key("messages", player["player_id"])
    messages = _BASE._get_json_setting(context, key, [])
    if not isinstance(messages, list):
        return []
    return [message for message in messages if isinstance(message, dict)]


def _save_messages_for_player(player, messages: list[dict[str, Any]], context) -> None:
    key = _canonical_player_setting_key("messages", player["player_id"])
    _BASE._set_json_setting(context, key, messages[-100:])


def _remove_recnet_messages(
    player,
    context,
    *,
    message_id: int | None = None,
    from_player_id: int | None = None,
    message_types: set[int] | None = None,
) -> list[int]:
    messages = _messages_for_player(player, context)
    kept: list[dict[str, Any]] = []
    removed: list[int] = []
    for message in messages:
        matches = True
        if message_id is not None:
            matches = int(message.get("Id") or 0) == message_id
        if matches and from_player_id is not None:
            matches = int(message.get("FromPlayerId") or 0) == from_player_id
        if matches and message_types is not None:
            raw_type = message.get("Type")
            matches = int(raw_type if raw_type is not None else -1) in message_types
        if matches:
            removed.append(int(message.get("Id") or 0))
        else:
            kept.append(message)
    if len(kept) != len(messages):
        _save_messages_for_player(player, kept, context)
    return [value for value in removed if value > 0]


async def _revoke_outgoing_game_invites(source_id: int, context) -> None:
    """Remove the sender's actionable game invites during room transitions."""
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT p.player_id, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE pvs.api_version = ? AND p.is_coach = 0
            """,
            (API_VERSION,),
        ).fetchall()

    for row in rows:
        try:
            state = json.loads(row["state_json"] or "{}")
        except Exception:
            state = {}
        if not isinstance(state, dict) or bool(state.get("deleted", False)):
            continue
        target_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
        if target_id <= 0 or target_id == source_id:
            continue
        target = {"player_id": row["player_id"], "state": state}
        removed_ids = _remove_recnet_messages(
            target,
            context,
            from_player_id=source_id,
            message_types={0, 3},
        )
        for message_id in removed_ids:
            # Queue cache invalidation offline and discard an unsent matching invite.
            _remove_pending_hub_notifications(
                target_id,
                context,
                notification_ids={2},
                message_id=message_id,
            )
            await _send_hub_notification(
                target_id,
                3,
                {"Id": message_id},
                context=context,
            )


async def _handle_get_relationships_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    return JSONResponse(_load_relationships(player, context))


async def _handle_relationship_type_change(
    request: Request, context, target_id: int, action: str
) -> Response:
    player = _authenticated_player(request, context)
    source_id = _legacy_id_for_player(player)
    if target_id == source_id:
        raise HTTPException(status_code=400, detail="A player cannot change their own relationship.")
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Player not found.")

    local_relationships = _load_relationships(player, context)
    remote_relationships = _load_relationships(target, context)
    local = _relationship_entry(local_relationships, target_id)
    remote = _relationship_entry(remote_relationships, source_id)
    if action == "sendfriendrequest":
        local["RelationshipType"] = 1
        remote["RelationshipType"] = 2
    elif action in {"acceptfriendrequest", "addfriend"}:
        local["RelationshipType"] = 3
        remote["RelationshipType"] = 3
    elif action == "removefriend":
        local["RelationshipType"] = 0
        remote["RelationshipType"] = 0
    else:
        raise HTTPException(status_code=404, detail="Unknown relationship action.")

    _save_relationships(player, local_relationships, context)
    _save_relationships(target, remote_relationships, context)
    await _notify_relationship(source_id, local, context)
    await _notify_relationship(target_id, remote, context)
    if action == "sendfriendrequest":
        # A retried request must refresh the same logical notification rather
        # than stacking duplicate popups in the recipient's durable inbox.
        _remove_recnet_messages(
            target,
            context,
            from_player_id=source_id,
            message_types={4},
        )
        # MessageType.FriendInvite (4) is the visible incoming request; the
        # RelationshipChanged event above updates the cached relationship.
        await _create_recnet_message(
            target,
            from_player_id=source_id,
            message_type=4,
            context=context,
        )
    elif action in {"acceptfriendrequest", "addfriend"}:
        # Remove the accepted invite so reconnect cannot restore it.
        _remove_recnet_messages(
            player,
            context,
            from_player_id=target_id,
            message_types={4},
        )
        await _create_recnet_message(
            target,
            from_player_id=source_id,
            message_type=40,
            context=context,
        )
    elif action == "removefriend":
        _remove_recnet_messages(
            player,
            context,
            from_player_id=target_id,
            message_types={4, 40},
        )
        _remove_recnet_messages(
            target,
            context,
            from_player_id=source_id,
            message_types={4, 40},
        )
    return JSONResponse(_relationship_dto(local))


async def _handle_relationship_flag_change(
    request: Request, context, target_id: int, field: str, enabled: bool
) -> Response:
    player = _authenticated_player(request, context)
    source_id = _legacy_id_for_player(player)
    if target_id == source_id:
        raise HTTPException(status_code=400, detail="A player cannot change their own relationship.")
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    local_relationships = _load_relationships(player, context)
    remote_relationships = _load_relationships(target, context)
    local = _relationship_entry(local_relationships, target_id)
    remote = _relationship_entry(remote_relationships, source_id)
    _set_reciprocal_status(local, remote, field, enabled)
    if field == "Ignored" and enabled:
    # Blocking uses reciprocal Ignored states and clears friendship state.
        local["RelationshipType"] = 0
        remote["RelationshipType"] = 0
        local["Favorited"] = 0
        remote["Favorited"] = 0
    _save_relationships(player, local_relationships, context)
    _save_relationships(target, remote_relationships, context)
    if field == "Ignored" and enabled:
        _remove_recnet_messages(
            player,
            context,
            from_player_id=target_id,
            message_types={4, 40},
        )
        _remove_recnet_messages(
            target,
            context,
            from_player_id=source_id,
            message_types={4, 40},
        )
    await _notify_relationship(source_id, local, context)
    await _notify_relationship(target_id, remote, context)
    return JSONResponse(_relationship_dto(local))


async def _handle_relationship_flag_post(
    request: Request, context, field: str, enabled: bool
) -> Response:
    payload = await _BASE._parse_client_payload(request)
    target_id = _BASE._int_field(
        payload, "PlayerId", "PlayerID", "playerId", "Id", "id", default=0
    )
    if target_id <= 0:
        raise HTTPException(status_code=400, detail="PlayerId is required.")
    return await _handle_relationship_flag_change(request, context, target_id, field, enabled)


async def _handle_get_messages_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    messages = _messages_for_player(player, context)
    relationship_by_id = {
        int(item.get("PlayerID") or 0): int(item.get("RelationshipType") or 0)
        for item in _load_relationships(player, context)
    }
    reconciled: list[dict[str, Any]] = []
    for message in messages:
        message_type = int(message.get("Type") or -1)
        sender_id = int(message.get("FromPlayerId") or 0)
        relationship_type = relationship_by_id.get(sender_id, 0)
        if message_type == 4 and relationship_type != 2:
            continue
        if message_type == 40 and relationship_type != 3:
            continue
        reconciled.append(message)
    if len(reconciled) != len(messages):
        _save_messages_for_player(player, reconciled, context)
    return JSONResponse(reconciled)


async def _handle_delete_message_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    message_id = _BASE._int_field(payload, "Id", "id", default=0)
    if message_id <= 0:
        raise HTTPException(status_code=400, detail="Id is required.")
    removed = _remove_recnet_messages(player, context, message_id=message_id)
    if removed:
        # Notifications.OnMessageDeleted deserializes exactly { Id }.
        await _send_hub_notification(
            _legacy_id_for_player(player),
            3,
            {"Id": message_id},
            context=context,
        )
    return Response(status_code=204)


async def _handle_get_player_subscriptions(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("player_subscriptions", player["player_id"])
    canonical_ids = _BASE._get_json_setting(context, key, [])
    if not isinstance(canonical_ids, list):
        canonical_ids = []
    subscriptions = []
    with context.db.connection() as conn:
        for canonical_id in canonical_ids:
            if str(canonical_id) == COACH_PLAYER_UUID:
                subscriptions.append({"PlayerId": 1})
                continue
            row = conn.execute(
                """
                SELECT pvs.state_json
                FROM player_version_state AS pvs
                WHERE pvs.player_id = ? AND pvs.api_version = ?
                """,
                (str(canonical_id), API_VERSION),
            ).fetchone()
            if row is None:
                continue
            try:
                state = json.loads(row["state_json"] or "{}")
            except Exception:
                state = {}
            legacy_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
            if legacy_id > 0:
                subscriptions.append({"PlayerId": legacy_id})
    return JSONResponse(subscriptions)


async def _handle_player_subscription_change(request: Request, context, target_id: int, *, subscribe: bool) -> Response:
    player = _authenticated_player(request, context)
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    key = _canonical_player_setting_key("player_subscriptions", player["player_id"])
    canonical_ids = _BASE._get_json_setting(context, key, [])
    if not isinstance(canonical_ids, list):
        canonical_ids = []
    target_canonical_id = str(target["player_id"])
    was_subscribed = target_canonical_id in {str(value) for value in canonical_ids}
    canonical_ids = [str(value) for value in canonical_ids if str(value) != target_canonical_id]
    if subscribe:
        canonical_ids.append(target_canonical_id)
    _BASE._set_json_setting(context, key, canonical_ids)
    changed = was_subscribed != subscribe
    if changed:
        player_state = _player_state(player)
        player_state["subscribed_count"] = max(
            0, int(player_state.get("subscribed_count", 0) or 0) + (1 if subscribe else -1)
        )
        _persist_player_state(player, player_state, context)
        if target_id == 1:
            current = int(
                _BASE._get_json_setting(context, f"{API_VERSION}:coach_subscriber_count", 0) or 0
            )
            _BASE._set_json_setting(
                context, f"{API_VERSION}:coach_subscriber_count", max(0, current + (1 if subscribe else -1))
            )
        else:
            target_state = _player_state(target)
            target_state["subscriber_count"] = max(
                0, int(target_state.get("subscriber_count", 0) or 0) + (1 if subscribe else -1)
            )
            _persist_player_state(target, target_state, context)
    # RecNet.PlayerSubscriptions parses UpdateResponse and only mutates its
    # local subscription cache when Response == Success (0).
    return JSONResponse({"Response": 0})


def _normalize_chat_message(message: dict[str, Any], thread_id: int) -> dict[str, Any]:
    normalized = dict(message)
    normalized["ChatThreadId"] = int(
        normalized.get("ChatThreadId", normalized.get("ThreadId", thread_id)) or thread_id
    )
    normalized.pop("ThreadId", None)
    contents = normalized.get("Contents")
    try:
        message_json = json.loads(contents) if isinstance(contents, str) else None
    except (TypeError, ValueError, json.JSONDecodeError):
        message_json = None
    # Repair messages written by the earlier adapter, which serialized a
    # client-provided MessageJson inside a second MessageJson.Data string.
    if isinstance(message_json, dict) and isinstance(message_json.get("Data"), str):
        try:
            nested = json.loads(message_json["Data"])
        except (TypeError, ValueError, json.JSONDecodeError):
            nested = None
        if isinstance(nested, dict) and {"Type", "Version", "Data"} <= set(nested):
            message_json = nested
    if not isinstance(message_json, dict) or not {"Type", "Version", "Data"} <= set(message_json):
        message_json = {"Type": 0, "Version": 1, "Data": str(contents or "")}
    normalized["Contents"] = json.dumps(message_json, separators=(",", ":"))
    normalized.pop("MessageJson", None)
    normalized.pop("TextMessageJson", None)
    return normalized


def _normalize_chat_thread(thread: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(thread)
    thread_id = int(normalized.get("ChatThreadId") or 0)
    normalized["ChatThreadName"] = str(normalized.get("ChatThreadName") or "")
    normalized.setdefault("SnoozedUntil", None)
    messages = normalized.get("Messages")
    normalized["Messages"] = [
        _normalize_chat_message(message, thread_id)
        for message in messages if isinstance(message, dict)
    ] if isinstance(messages, list) else []
    if isinstance(normalized.get("LatestMessage"), dict):
        normalized["LatestMessage"] = _normalize_chat_message(
            normalized["LatestMessage"], thread_id
        )
    normalized["PlayerIds"] = [
        int(value) for value in normalized.get("PlayerIds", [])
        if str(value).lstrip("-").isdigit() and int(value) > 0
    ]
    if not isinstance(normalized.get("LastReadByPlayer"), dict):
        normalized["LastReadByPlayer"] = {}
    if not isinstance(normalized.get("SnoozedUntilByPlayer"), dict):
        normalized["SnoozedUntilByPlayer"] = {}
    return normalized


def _chat_thread_response(
    thread: dict[str, Any],
    player_id: int,
    *,
    message_count: int | None = None,
    summary: bool = False,
) -> dict[str, Any]:
    normalized = _normalize_chat_thread(thread)
    read_by_player = normalized.get("LastReadByPlayer", {})
    snoozed_by_player = normalized.get("SnoozedUntilByPlayer", {})
    messages = normalized.get("Messages", [])
    latest = normalized.get("LatestMessage")
    if not isinstance(latest, dict) and messages:
        latest = messages[-1]
    response = {
        "ChatThreadId": int(normalized.get("ChatThreadId") or 0),
        "ChatThreadName": str(normalized.get("ChatThreadName") or ""),
        "LastReadMessageId": int(
            read_by_player.get(
                str(player_id), normalized.get("LastReadMessageId", 0)
            )
            or 0
        ),
        "SnoozedUntil": snoozed_by_player.get(
            str(player_id), normalized.get("SnoozedUntil")
        ),
        "PlayerIds": list(normalized.get("PlayerIds", [])),
    }
    if summary:
    # Omit Messages so the client falls back to LatestMessage safely.
        if isinstance(latest, dict):
            response["LatestMessage"] = latest
    else:
        selected = messages
        if message_count is not None:
            selected = selected[-max(0, message_count):] if message_count else []
        response["Messages"] = selected
        if isinstance(latest, dict):
            response["LatestMessage"] = latest
    return response


def _global_chat_threads(context) -> list[dict[str, Any]]:
    threads = _BASE._get_json_setting(context, CHAT_THREADS_SETTING, [])
    return [
        _normalize_chat_thread(item) for item in threads if isinstance(item, dict)
    ] if isinstance(threads, list) else []


def _save_global_chat_threads(context, threads: list[dict[str, Any]]) -> None:
    _BASE._set_json_setting(context, CHAT_THREADS_SETTING, threads[-200:])


def _chat_player_ids(request: Request, payload: dict[str, Any] | None = None) -> list[int]:
    raw_values: list[Any] = []
    if payload is not None:
        raw = payload.get("PlayerIds", payload.get("playerIds", []))
        raw_values.extend(raw if isinstance(raw, list) else [raw])
    else:
        raw_values.extend(request.query_params.getlist("PlayerIds"))
        raw_values.extend(request.query_params.getlist("playerIds"))
    result: list[int] = []
    for raw in raw_values:
        for part in re.split(r"[,;\s]+", str(raw or "")):
            if not part or not part.lstrip("-").isdigit():
                continue
            value = int(part)
            if value > 0 and value not in result:
                result.append(value)
    return result


def _chat_thread_for_players(
    context, player_ids: list[int], *, create: bool
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    normalized = sorted(set(player_ids))
    threads = _global_chat_threads(context)
    for thread in threads:
        existing = thread.get("PlayerIds")
        if isinstance(existing, list) and sorted({int(value) for value in existing}) == normalized:
            return thread, threads
    if not create:
        return None, threads
    next_id = max((int(item.get("ChatThreadId") or 0) for item in threads), default=0) + 1
    thread = {
        "ChatThreadId": next_id,
        "ChatThreadName": "",
        "LastReadMessageId": 0,
        "SnoozedUntil": None,
        "Messages": [],
        "PlayerIds": normalized,
        "LastReadByPlayer": {str(player_id): 0 for player_id in normalized},
        "SnoozedUntilByPlayer": {},
        "RecentlyLeftPlayerIds": [],
        "MayNeedNewFetch": False,
        "MessagesUpToDateThroughMessageID": 0,
    }
    threads.append(thread)
    return thread, threads


def _new_chat_message(thread_id: int, sender_id: int, contents: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    message_id = int(now.timestamp() * 1_000_000) * 1000 + secrets.randbelow(1000)
    try:
        parsed = json.loads(contents)
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = None
    message_json = (
        parsed
        if isinstance(parsed, dict) and {"Type", "Version", "Data"} <= set(parsed)
        else {"Type": 0, "Version": 1, "Data": contents}
    )
    return {
        "ChatMessageId": message_id,
        "ChatThreadId": thread_id,
        "SenderPlayerId": sender_id,
        "TimeSent": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "Contents": json.dumps(message_json, separators=(",", ":")),
    }


async def _handle_get_chat_threads(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    player_id = _legacy_id_for_player(player)
    threads = [
        thread
        for thread in _global_chat_threads(context)
        if player_id in {int(value) for value in thread.get("PlayerIds", [])}
        and (
            bool(thread.get("Messages"))
            or isinstance(thread.get("LatestMessage"), dict)
        )
    ]
    threads.sort(
        key=lambda item: int(item.get("MessagesUpToDateThroughMessageID") or 0), reverse=True
    )
    requested_count = max(
        1, min(_BASE._int_field(dict(request.query_params), "count", "Count", default=50), 100)
    )
    return JSONResponse(
        [
            _chat_thread_response(thread, player_id, summary=True)
            for thread in threads[:requested_count]
        ]
    )


async def _handle_get_chat_by_players(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    player_ids = _chat_player_ids(request)
    if local_id not in player_ids:
        player_ids.append(local_id)
    if len(set(player_ids)) < 2:
        raise HTTPException(status_code=400, detail="At least two PlayerIds are required.")
    for player_id in player_ids:
        if _find_player_by_legacy_id_25april2019(context, player_id) is None:
            raise HTTPException(status_code=404, detail=f"Player {player_id} not found.")
    with _CHAT_LOCK:
        thread, _ = _chat_thread_for_players(context, player_ids, create=False)
        if thread is None or (
            not thread.get("Messages")
            and not isinstance(thread.get("LatestMessage"), dict)
        ):
            raise HTTPException(status_code=404, detail="Chat thread not found.")
        message_count = max(
            1, min(_BASE._int_field(dict(request.query_params), "MessageCount", "messageCount", default=50), 100)
        )
        response_thread = _chat_thread_response(thread, local_id, message_count=message_count)
    return JSONResponse(response_thread)


async def _handle_create_chat(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    local_id = _legacy_id_for_player(player)
    player_ids = _chat_player_ids(request, payload)
    if local_id not in player_ids:
        player_ids.append(local_id)
    contents = _BASE._str_field(payload, "MessageContents", "messageContents").strip()
    if len(set(player_ids)) < 2 or not contents:
        return JSONResponse({"ChatThread": None, "ChatResult": 1})
    if not _valid_chat_contents(contents):
        return JSONResponse({"ChatThread": None, "ChatResult": 6})
    if not _user_text_is_pure(
        context,
        contents,
        field_context="chat.message",
        player=player,
    ):
        return JSONResponse({"ChatThread": None, "ChatResult": 6})
    for player_id in player_ids:
        if _find_player_by_legacy_id_25april2019(context, player_id) is None:
            return JSONResponse({"ChatThread": None, "ChatResult": 2})
        if player_id != local_id and _players_ignore_each_other(context, local_id, player_id):
            return JSONResponse({"ChatThread": None, "ChatResult": 5})
    with _CHAT_LOCK:
        thread, threads = _chat_thread_for_players(context, player_ids, create=True)
        message = _new_chat_message(int(thread["ChatThreadId"]), local_id, contents)
        messages = thread.get("Messages") if isinstance(thread.get("Messages"), list) else []
        messages.append(message)
        thread["Messages"] = messages[-100:]
        thread["LatestMessage"] = message
        thread["MessagesUpToDateThroughMessageID"] = int(message["ChatMessageId"])
        read_by_player = thread.get("LastReadByPlayer")
        if not isinstance(read_by_player, dict):
            read_by_player = {}
        read_by_player[str(local_id)] = int(message["ChatMessageId"])
        thread["LastReadByPlayer"] = read_by_player
        _save_global_chat_threads(context, threads)
    for target_id in thread["PlayerIds"]:
    # Notify the sender too; the HTTP response does not update its chat cache.
        await _send_hub_notification(int(target_id), 90, message, context=context)
    return JSONResponse({"ChatThread": _chat_thread_response(thread, local_id), "ChatResult": 0})


def _chat_id(payload: dict[str, Any]) -> int:
    return _BASE._int_field(payload, "ChatId", "chatId", default=0)


def _chat_thread_by_id(
    context, chat_id: int
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    threads = _global_chat_threads(context)
    return next((thread for thread in threads if int(thread.get("ChatThreadId") or 0) == chat_id), None), threads


def _valid_chat_contents(contents: str) -> bool:
    return bool(contents.strip()) and len(contents) <= 2048 and not any(
        ord(character) < 32 and character not in "\t\r\n" for character in contents
    )


async def _handle_get_chat_by_id(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    query = dict(request.query_params)
    chat_id = _chat_id(query)
    message_count = max(1, min(_BASE._int_field(query, "MessageCount", "messageCount", default=50), 100))
    with _CHAT_LOCK:
        thread, _ = _chat_thread_by_id(context, chat_id)
        if thread is None:
            raise HTTPException(status_code=404, detail="Chat thread not found.")
        if local_id not in {int(value) for value in thread.get("PlayerIds", [])}:
            raise HTTPException(status_code=403, detail="The player is not a member of this chat.")
        if not thread.get("Messages") and not isinstance(thread.get("LatestMessage"), dict):
            raise HTTPException(status_code=404, detail="Chat thread not found.")
        return JSONResponse(_chat_thread_response(thread, local_id, message_count=message_count))


async def _handle_get_chat_messages(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    query = dict(request.query_params)
    chat_id = _chat_id(query)
    mode = _BASE._int_field(query, "Mode", "mode", default=0)
    reference_id = _BASE._int_field(query, "ReferenceMessageId", "referenceMessageId", default=0)
    count = max(1, min(_BASE._int_field(query, "MessageCount", "messageCount", default=100), 100))
    with _CHAT_LOCK:
        thread, _ = _chat_thread_by_id(context, chat_id)
        if thread is None:
            raise HTTPException(status_code=404, detail="Chat thread not found.")
        if local_id not in {int(value) for value in thread.get("PlayerIds", [])}:
            raise HTTPException(status_code=403, detail="The player is not a member of this chat.")
        messages = [
            _normalize_chat_message(message, chat_id)
            for message in thread.get("Messages", []) if isinstance(message, dict)
        ]
        if mode == 1:
            selected = [message for message in messages if int(message["ChatMessageId"]) > reference_id][:count]
        elif mode == 2:
            selected = [message for message in messages if int(message["ChatMessageId"]) < reference_id][-count:]
        else:
            selected = messages[-count:]
    return JSONResponse(selected)


async def _handle_send_chat_message(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    payload = await _BASE._parse_client_payload(request)
    chat_id = _chat_id(payload)
    contents = _BASE._str_field(payload, "MessageContents", "messageContents")
    if chat_id <= 0 or not contents:
        return JSONResponse({"ChatMessage": None, "ChatResult": 1})
    if not _valid_chat_contents(contents):
        return JSONResponse({"ChatMessage": None, "ChatResult": 6})
    if not _user_text_is_pure(
        context,
        contents,
        field_context="chat.message",
        player=player,
    ):
        return JSONResponse({"ChatMessage": None, "ChatResult": 6})
    with _CHAT_LOCK:
        thread, threads = _chat_thread_by_id(context, chat_id)
        if thread is None:
            return JSONResponse({"ChatMessage": None, "ChatResult": 2})
        if local_id not in {int(value) for value in thread.get("PlayerIds", [])}:
            return JSONResponse({"ChatMessage": None, "ChatResult": 3})
        if any(
            target_id != local_id
            and _players_ignore_each_other(context, local_id, target_id)
            for target_id in {
                int(value) for value in thread.get("PlayerIds", [])
            }
        ):
            return JSONResponse({"ChatMessage": None, "ChatResult": 5})
        message = _new_chat_message(chat_id, local_id, contents)
        messages = thread.get("Messages") if isinstance(thread.get("Messages"), list) else []
        messages.append(message)
        thread["Messages"] = messages[-100:]
        thread["LatestMessage"] = message
        thread["MessagesUpToDateThroughMessageID"] = int(message["ChatMessageId"])
        read_by_player = thread.get("LastReadByPlayer")
        if not isinstance(read_by_player, dict):
            read_by_player = {}
        read_by_player[str(local_id)] = int(message["ChatMessageId"])
        thread["LastReadByPlayer"] = read_by_player
        _save_global_chat_threads(context, threads)
        recipients = [int(value) for value in thread.get("PlayerIds", [])]
    for target_id in recipients:
        await _send_hub_notification(target_id, 90, message, context=context)
    return JSONResponse({"ChatMessage": message, "ChatResult": 0})


async def _handle_read_chat_message(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    payload = await _BASE._parse_client_payload(request)
    chat_id = _chat_id(payload)
    message_id = _BASE._int_field(payload, "MessageId", "messageId", default=0)
    with _CHAT_LOCK:
        thread, threads = _chat_thread_by_id(context, chat_id)
        if thread is None:
            return JSONResponse({"ChatResult": 2})
        if local_id not in {int(value) for value in thread.get("PlayerIds", [])}:
            return JSONResponse({"ChatResult": 3})
        message_ids = {
            int(message.get("ChatMessageId") or 0)
            for message in thread.get("Messages", []) if isinstance(message, dict)
        }
        if message_id <= 0 or message_id not in message_ids:
            return JSONResponse({"ChatResult": 1})
        read_by_player = thread.get("LastReadByPlayer")
        if not isinstance(read_by_player, dict):
            read_by_player = {}
        read_by_player[str(local_id)] = max(int(read_by_player.get(str(local_id), 0) or 0), message_id)
        thread["LastReadByPlayer"] = read_by_player
        _save_global_chat_threads(context, threads)
    _remove_pending_hub_notifications(local_id, context, notification_ids={90}, message_id=message_id)
    return JSONResponse({"ChatResult": 0})


async def _handle_add_to_chat(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    payload = await _BASE._parse_client_payload(request)
    chat_id = _chat_id(payload)
    target_id = _BASE._int_field(payload, "PlayerToAddId", "playerToAddId", default=0)
    if target_id <= 0:
        return JSONResponse({"ChatResult": 1})
    if _find_player_by_legacy_id_25april2019(context, target_id) is None:
        return JSONResponse({"ChatResult": 5})
    if _players_ignore_each_other(context, local_id, target_id):
        return JSONResponse({"ChatResult": 5})
    with _CHAT_LOCK:
        thread, threads = _chat_thread_by_id(context, chat_id)
        if thread is None:
            return JSONResponse({"ChatResult": 2})
        members = [int(value) for value in thread.get("PlayerIds", [])]
        if local_id not in members:
            return JSONResponse({"ChatResult": 3})
        if target_id in members:
            return JSONResponse({"ChatResult": 4})
        members.append(target_id)
        thread["PlayerIds"] = sorted(set(members))
        read_by_player = thread.get("LastReadByPlayer")
        if not isinstance(read_by_player, dict):
            read_by_player = {}
        read_by_player[str(target_id)] = 0
        thread["LastReadByPlayer"] = read_by_player
        _save_global_chat_threads(context, threads)
    return JSONResponse({"ChatResult": 0})


async def _handle_leave_chat(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    payload = await _BASE._parse_client_payload(request)
    chat_id = _chat_id(payload)
    with _CHAT_LOCK:
        thread, threads = _chat_thread_by_id(context, chat_id)
        if thread is None:
            return JSONResponse({"ChatResult": 2})
        members = [int(value) for value in thread.get("PlayerIds", [])]
        if local_id not in members:
            return JSONResponse({"ChatResult": 3})
        thread["PlayerIds"] = [value for value in members if value != local_id]
        recently_left = thread.get("RecentlyLeftPlayerIds")
        if not isinstance(recently_left, list):
            recently_left = []
        thread["RecentlyLeftPlayerIds"] = [*{int(value) for value in recently_left}, local_id]
        _save_global_chat_threads(context, threads)
    return JSONResponse({"ChatResult": 0})


async def _handle_rename_chat(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    payload = await _BASE._parse_client_payload(request)
    chat_id = _chat_id(payload)
    chat_name = _BASE._str_field(payload, "ChatName", "chatName").strip()
    if not chat_name or len(chat_name) > 80:
        return JSONResponse({"ChatResult": 6})
    chat_name = _filter_user_text(
        context,
        chat_name,
        policy="censor",
        field_context="chat.thread_name",
        player=player,
    )
    with _CHAT_LOCK:
        thread, threads = _chat_thread_by_id(context, chat_id)
        if thread is None:
            return JSONResponse({"ChatResult": 2})
        if local_id not in {int(value) for value in thread.get("PlayerIds", [])}:
            return JSONResponse({"ChatResult": 3})
        thread["ChatThreadName"] = chat_name
        _save_global_chat_threads(context, threads)
    return JSONResponse({"ChatResult": 0})


async def _handle_snooze_chat(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    local_id = _legacy_id_for_player(player)
    payload = await _BASE._parse_client_payload(request)
    chat_id = _chat_id(payload)
    snooze = _bool_value(payload.get("Snooze", payload.get("snooze", False)))
    with _CHAT_LOCK:
        thread, threads = _chat_thread_by_id(context, chat_id)
        if thread is None:
            return JSONResponse({"ChatResult": 2, "SnoozedUntil": None})
        if local_id not in {int(value) for value in thread.get("PlayerIds", [])}:
            return JSONResponse({"ChatResult": 3, "SnoozedUntil": None})
        snoozed = thread.get("SnoozedUntilByPlayer")
        if not isinstance(snoozed, dict):
            snoozed = {}
        snoozed_until = (
            (datetime.now(timezone.utc) + timedelta(days=365)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            if snooze else None
        )
        if snoozed_until is None:
            snoozed.pop(str(local_id), None)
        else:
            snoozed[str(local_id)] = snoozed_until
        thread["SnoozedUntilByPlayer"] = snoozed
        _save_global_chat_threads(context, threads)
    return JSONResponse({"ChatResult": 0, "SnoozedUntil": snoozed_until})


async def _handle_bulk_ignore_platform_users(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    raw_ids = payload.get("PlatformIds", payload.get("platformIds", []))
    if isinstance(raw_ids, str):
        raw_ids = [part for part in re.split(r"[,;\s]+", raw_ids) if part]
    if not isinstance(raw_ids, list):
        raw_ids = []
    key = _canonical_player_setting_key("ignored_platform_users", player["player_id"])
    existing = _BASE._get_json_setting(context, key, [])
    ignored = {str(value) for value in existing} if isinstance(existing, list) else set()
    ignored.update(str(value) for value in raw_ids if str(value).strip())
    _BASE._set_json_setting(context, key, sorted(ignored))
    return Response(status_code=204)


async def _handle_loading_screen_tips(context) -> Response:
    tips = context.get_server_setting("loading_screen_tips", [])
    return JSONResponse(tips if isinstance(tips, list) else [])


async def _handle_named_images(context) -> Response:
    images = context.get_server_setting("named_images", [])
    return JSONResponse(images if isinstance(images, list) else [])


async def _handle_charades_words(request: Request, context) -> Response:
    _authenticated_player(request, context)
    # CardBox.GetWordsAsync deserializes a bare List<CharadesWord>. In this
    # build each word has only Difficulty (Easy=0, Hard=1) and EN_US.
    return JSONResponse(
        [{"Difficulty": difficulty, "EN_US": word} for word, difficulty in CHARADES_WORDS]
    )


def _test_management_passes(context) -> list[dict[str, Any]]:
    # Keep the exact client contract available without inventing private
    # RecNet campaign content that is absent from the April 2019 build.
    return []


def _normalize_test_case(value: dict[str, Any]) -> dict[str, Any]:
    assigned_ids = sorted(
        {
            int(item) for item in value.get("AssignedPlayerIds", [])
            if str(item).lstrip("-").isdigit() and int(item) > 0
        }
    )
    assigned_names = [str(item) for item in value.get("AssignedPlayerNames", [])]
    status = int(value.get("Status", 0) or 0)
    return {
        "Id": str(value.get("Id") or ""),
        "Key": str(value.get("Key") or ""),
        "Title": str(value.get("Title") or ""),
        "Description": str(value.get("Description") or ""),
        "Status": status if status in {0, 1, 2, 3} else 0,
        "MinNumAssignedPlayers": max(1, int(value.get("MinNumAssignedPlayers", 1) or 1)),
        "AssignedPlayerIds": assigned_ids,
        "AssignedPlayerNames": assigned_names[: len(assigned_ids)],
        "Tags": [str(item) for item in value.get("Tags", []) if str(item).strip()],
        "JiraUrl": str(value.get("JiraUrl") or ""),
        "JiraBugUrl": str(value.get("JiraBugUrl") or ""),
    }


def _normalize_test_pass(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "Id": int(value.get("Id", 0) or 0),
        "Name": str(value.get("Name") or ""),
        "Description": str(value.get("Description") or ""),
        "StartDate": str(value.get("StartDate") or ""),
        "EndDate": str(value.get("EndDate") or ""),
        "WasManuallyClosed": bool(value.get("WasManuallyClosed", False)),
        "TestCases": [
            _normalize_test_case(item)
            for item in value.get("TestCases", []) if isinstance(item, dict)
        ],
        "Tags": [str(item) for item in value.get("Tags", []) if str(item).strip()],
    }


def _find_test_case(
    passes: list[dict[str, Any]], test_case_id: str
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    target = test_case_id.casefold()
    for test_pass in passes:
        for test_case in test_pass.get("TestCases", []):
            if isinstance(test_case, dict) and str(test_case.get("Id") or "").casefold() == target:
                return test_pass, test_case
    return None


async def _handle_test_pass_summaries(request: Request, context) -> Response:
    _authenticated_player(request, context)
    # The private campaign content is not in the client. An unconfigured
    # service truthfully returns no passes; configured passes use the real DTO.
    return JSONResponse([_normalize_test_pass(item) for item in _test_management_passes(context)])


async def _handle_get_test_pass(test_pass_id: int, request: Request, context) -> Response:
    _authenticated_player(request, context)
    test_pass = next(
        (
            item for item in _test_management_passes(context)
            if int(item.get("Id", 0) or 0) == test_pass_id
        ),
        None,
    )
    if test_pass is None:
        raise HTTPException(status_code=404, detail="Test pass not found.")
    return JSONResponse(_normalize_test_pass(test_pass))


async def _handle_get_test_case(test_case_id: str, request: Request, context) -> Response:
    _authenticated_player(request, context)
    found = _find_test_case(_test_management_passes(context), test_case_id)
    if found is None:
        raise HTTPException(status_code=404, detail="Test case not found.")
    return JSONResponse(_normalize_test_case(found[1]))


async def _handle_mutate_test_case(
    test_case_id: str, action: str, request: Request, context
) -> Response:
    player = _authenticated_player(request, context)
    player_id = _legacy_id_for_player(player)
    passes = _test_management_passes(context)
    found = _find_test_case(passes, test_case_id)
    if found is None:
        raise HTTPException(status_code=404, detail="Test case not found.")
    test_case = found[1]
    assigned_ids = [
        int(item) for item in test_case.get("AssignedPlayerIds", [])
        if str(item).lstrip("-").isdigit() and int(item) > 0
    ]
    assigned_names = [str(item) for item in test_case.get("AssignedPlayerNames", [])]
    if action == "claim":
        if player_id not in assigned_ids:
            assigned_ids.append(player_id)
            assigned_names.append(str(player["display_name"] or player["username"] or ""))
        if int(test_case.get("Status", 0) or 0) == 0:
            test_case["Status"] = 1
    elif action == "unclaim":
        kept = [
            (assigned_id, assigned_names[index] if index < len(assigned_names) else "")
            for index, assigned_id in enumerate(assigned_ids) if assigned_id != player_id
        ]
        assigned_ids = [item[0] for item in kept]
        assigned_names = [item[1] for item in kept]
        if not assigned_ids and int(test_case.get("Status", 0) or 0) == 1:
            test_case["Status"] = 0
    else:
        payload = await _BASE._parse_client_payload(request)
        status = _BASE._int_field(payload, "Status", "NewStatus", default=-1)
        if status not in {0, 1, 2, 3}:
            raise HTTPException(status_code=400, detail="Status must be between 0 and 3.")
        test_case["Status"] = status
    test_case["AssignedPlayerIds"] = assigned_ids
    test_case["AssignedPlayerNames"] = assigned_names
    return Response(status_code=204)


def _room_engagement_sort_key(record: dict[str, Any]) -> tuple[int, int, int, int, str]:
    visits = int(record["metadata"].get("visit_count", 0) or 0)
    favorites = int(record["metadata"].get("favorite_count", 0) or 0)
    cheers = int(record["metadata"].get("cheer_count", 0) or 0)
    return (
        visits + favorites + cheers,
        visits,
        favorites,
        cheers,
        str(record["row"]["updated_at"]),
    )


def _video_frame_luma(ffmpeg: str, video_path: Path, timestamp: float) -> float | None:
    try:
        result = subprocess.run(
            [
                ffmpeg,
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                f"{timestamp:.6f}",
                "-i",
                str(video_path),
                "-frames:v",
                "1",
                "-vf",
                "scale=64:-2,signalstats,metadata=print:file=-",
                "-an",
                "-f",
                "null",
                "-",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    values = re.findall(
        r"lavfi\.signalstats\.YAVG=([0-9.]+)",
        result.stdout + result.stderr,
    )
    if not values:
        return None
    return sum(float(value) for value in values) / len(values)


def _community_board_thumbnail_time(
    ffmpeg: str,
    video_path: Path,
    duration: float,
) -> float:
    midpoint = duration / 2.0
    best_timestamp = midpoint
    best_luma = -1.0
    seen: set[float] = set()
    for position in COMMUNITY_BOARD_THUMBNAIL_POSITIONS:
        timestamp = min(max(duration * position, 0.0), max(0.0, duration - 0.05))
        timestamp_key = round(timestamp, 3)
        if timestamp_key in seen:
            continue
        seen.add(timestamp_key)
        luma = _video_frame_luma(ffmpeg, video_path, timestamp)
        if luma is None:
            continue
        if luma > best_luma:
            best_timestamp = timestamp
            best_luma = luma
        if luma >= COMMUNITY_BOARD_THUMBNAIL_MIN_LUMA:
            return timestamp
    return best_timestamp


def _community_board_video_thumbnail(video_path: Path, thumbnail_dir: Path) -> str:
    with _COMMUNITY_BOARD_THUMBNAIL_LOCK:
        return _community_board_video_thumbnail_locked(video_path, thumbnail_dir)


def _community_board_video_thumbnail_locked(video_path: Path, thumbnail_dir: Path) -> str:
    safe_stem = re.sub(r"[^A-Za-z0-9_-]+", "-", video_path.stem).strip("-")[:48]
    cache_identity = (
        f"{COMMUNITY_BOARD_THUMBNAIL_CACHE_VERSION}:{video_path.name.casefold()}"
    )
    digest = hashlib.sha256(cache_identity.encode("utf-8")).hexdigest()[:12]
    thumbnail_name = f"CommunityBoardVideo_{safe_stem or 'video'}_{digest}.png"
    thumbnail_path = thumbnail_dir / thumbnail_name
    try:
        if (
            thumbnail_path.is_file()
            and thumbnail_path.stat().st_mtime_ns >= video_path.stat().st_mtime_ns
        ):
            return thumbnail_name
    except OSError:
        pass

    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if not ffmpeg or not ffprobe:
        return thumbnail_name if thumbnail_path.is_file() else DEFAULT_IMAGE_NAME
    temporary_path = thumbnail_path.with_name(
        f".{thumbnail_path.stem}.{secrets.token_hex(6)}.tmp.png"
    )
    try:
        duration_result = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        duration = max(0.0, float(duration_result.stdout.strip()))
        frame_time = _community_board_thumbnail_time(ffmpeg, video_path, duration)
        detect_start = max(0.0, frame_time - 1.0)
        detect_duration = min(2.0, max(0.1, duration - detect_start))
        crops = []
        try:
            crop_result = subprocess.run(
                [
                    ffmpeg,
                    "-hide_banner",
                    "-ss",
                    f"{detect_start:.6f}",
                    "-i",
                    str(video_path),
                    "-t",
                    f"{detect_duration:.6f}",
                    "-vf",
                    "cropdetect=limit=24:round=2:reset=0",
                    "-an",
                    "-f",
                    "null",
                    "-",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            crops = re.findall(r"\bcrop=(\d+:\d+:\d+:\d+)", crop_result.stderr)
        except (OSError, subprocess.SubprocessError):
            pass
        crop = Counter(crops).most_common(1)[0][0] if crops else ""
        thumbnail_dir.mkdir(parents=True, exist_ok=True)
        command = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{frame_time:.6f}",
            "-i",
            str(video_path),
            "-frames:v",
            "1",
        ]
        if crop:
            command.extend(["-vf", f"crop={crop}"])
        command.extend(["-y", str(temporary_path)])
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            timeout=30,
        )
        temporary_path.replace(thumbnail_path)
        return thumbnail_name
    except (OSError, ValueError, subprocess.SubprocessError):
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        return thumbnail_name if thumbnail_path.is_file() else DEFAULT_IMAGE_NAME


def _community_board_videos(context) -> list[dict[str, Any]]:
    if not COMMUNITY_BOARD_VIDEO_DIR.is_dir():
        return []
    thumbnail_dir = context.data_dir / COMMUNITY_BOARD_THUMBNAIL_SUBDIR
    files = {
        child.name.casefold(): child
        for child in COMMUNITY_BOARD_VIDEO_DIR.iterdir()
        if child.is_file() and child.suffix.casefold() in COMMUNITY_BOARD_VIDEO_EXTENSIONS
    }
    catalog = {
        str(entry.get("FileName") or "").casefold(): entry
        for entry in COMMUNITY_BOARD_VIDEO_CATALOG
        if isinstance(entry, dict) and str(entry.get("FileName") or "").strip()
    }
    videos = []
    for key, path in sorted(files.items()):
        entry = catalog.get(key, {})
        configured_thumbnail = str(entry.get("ThumbnailImageName") or "").strip()
        videos.append(
            {
                "BlobName": path.name,
                "Title": str(entry.get("Title") or path.stem.replace("_", " ")),
                "Description": str(entry.get("Description") or ""),
                "ThumbnailBlobName": (
                    configured_thumbnail
                    or _community_board_video_thumbnail(path, thumbnail_dir)
                ),
                "SourceUrl": str(entry.get("SourceUrl") or ""),
            }
        )
    if len(videos) > 1:
        # Rotate which local video appears first every five minutes while still
        # returning the complete catalog for the client's bulletin-board cards.
        offset = int(datetime.now(timezone.utc).timestamp() // 300) % len(videos)
        videos = videos[offset:] + videos[:offset]
    return videos


async def _handle_community_board(context) -> Response:
    featured_records = _all_ugc_records(context, public_only=True)
    featured_records.sort(key=_room_engagement_sort_key, reverse=True)
    videos = await asyncio.to_thread(_community_board_videos, context)
    featured_rooms = [
        {
            "RoomName": str(record["metadata"].get("name") or record["row"]["name"]),
            "RoomId": int(record["version"]["room_id"]),
            "ImageName": str(record["metadata"].get("image_name") or DEFAULT_IMAGE_NAME),
        }
        for record in featured_records
    ]
    return JSONResponse(
        {
            "FeaturedPlayer": {"Id": 0, "TitleOverride": "", "UrlOverride": ""},
            "FeaturedRoomGroup": {
                "Name": "Featured Rooms",
                "FeaturedRooms": featured_rooms,
            },
            "CurrentAnnouncement": {
                "Message": "Welcome to Rec Room Patches!",
                "MoreInfoUrl": "",
            },
            "InstagramImages": [],
            "Videos": videos,
        },
        headers={"Cache-Control": "no-store"},
    )


async def _handle_community_board_video(video_name: str) -> Response:
    if Path(video_name).name != video_name:
        raise HTTPException(status_code=404, detail="Community board video not found.")
    video_path = (COMMUNITY_BOARD_VIDEO_DIR / video_name).resolve()
    video_root = COMMUNITY_BOARD_VIDEO_DIR.resolve()
    if (
        video_root not in video_path.parents
        or video_path.suffix.casefold() not in COMMUNITY_BOARD_VIDEO_EXTENSIONS
        or not video_path.is_file()
    ):
        raise HTTPException(status_code=404, detail="Community board video not found.")
    media_type = {
        ".mov": "video/quicktime",
        ".mp4": "video/mp4",
        ".m4v": "video/x-m4v",
    }[video_path.suffix.casefold()]
    return Response(
        content=video_path.read_bytes(),
        media_type=media_type,
        headers={"Cache-Control": "no-cache"},
    )


async def _handle_quick_play(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    quick_play = await context.require_transient().take_json(
        f"{API_VERSION}-quick-play", player["player_id"]
    )
    if not isinstance(quick_play, dict):
        quick_play = {"RoomName": "", "TargetPlayerId": 0}
    return JSONResponse(
        {
            "RoomName": str(quick_play.get("RoomName") or ""),
            "TargetPlayerId": int(quick_play.get("TargetPlayerId") or 0),
        }
    )


async def _handle_player_events(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    player_id = _legacy_id_for_player(player)
    all_events = _visible_player_events(context)
    created = [event for event in all_events if int(event.get("CreatorPlayerId", 0) or 0) == player_id]
    events_by_id = {int(event.get("PlayerEventId", 0) or 0): event for event in all_events}
    responses = [
        {"PlayerEvent": events_by_id[event_id], "PlayerEventResponse": response}
        for response in _global_player_event_responses(context)
        if int(response.get("PlayerId", 0) or 0) == player_id
        if (event_id := int(response.get("PlayerEventId", 0) or 0)) in events_by_id
    ]
    created.sort(key=lambda event: str(event.get("StartTime") or ""))
    responses.sort(key=lambda item: str(item["PlayerEvent"].get("StartTime") or ""))
    return JSONResponse({"Created": created, "Responses": responses})


def _global_player_events(context) -> list[dict[str, Any]]:
    events = _BASE._get_json_setting(context, f"{API_VERSION}:player_events_global", [])
    return [item for item in events if isinstance(item, dict)] if isinstance(events, list) else []


def _visible_player_events(context) -> list[dict[str, Any]]:
    return [
        event
        for event in _global_player_events(context)
        if not context.is_content_quarantined(
            "player_event",
            int(event.get("PlayerEventId", 0) or 0),
        )
    ]


def _save_global_player_events(context, events: list[dict[str, Any]]) -> None:
    _BASE._set_json_setting(context, f"{API_VERSION}:player_events_global", events)


def _update_event_images_for_room(context, room_id: int, image_name: str) -> None:
    """Keep PlayerEvent's room-derived image snapshot in sync with the room."""
    events = _global_player_events(context)
    changed = False
    for event in events:
        if int(event.get("RoomId", 0) or 0) != room_id:
            continue
        if str(event.get("ImageName") or "") == image_name:
            continue
        event["ImageName"] = image_name
        changed = True
    if changed:
        _save_global_player_events(context, events)


def _global_player_event_responses(context) -> list[dict[str, Any]]:
    responses = _BASE._get_json_setting(context, f"{API_VERSION}:player_event_responses_global", [])
    return [item for item in responses if isinstance(item, dict)] if isinstance(responses, list) else []


def _save_global_player_event_responses(context, responses: list[dict[str, Any]]) -> None:
    _BASE._set_json_setting(context, f"{API_VERSION}:player_event_responses_global", responses)


def _event_by_id(event_id: int, context) -> dict[str, Any] | None:
    return next(
        (
            event
            for event in _visible_player_events(context)
            if int(event.get("PlayerEventId", 0) or 0) == event_id
        ),
        None,
    )


def _refresh_event_attendee_count(event_id: int, context) -> dict[str, Any] | None:
    events = _global_player_events(context)
    event = next((item for item in events if int(item.get("PlayerEventId", 0) or 0) == event_id), None)
    if event is None:
        return None
    event["AttendeeCount"] = sum(
        1
        for response in _global_player_event_responses(context)
        if int(response.get("PlayerEventId", 0) or 0) == event_id and int(response.get("Type", -1)) == 0
    )
    _save_global_player_events(context, events)
    return event


def _parse_recnet_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    # .NET's round-trip "o" format carries seven fractional digits; Python
    # stores microseconds, so trim only the extra precision before parsing.
    text = re.sub(r"(\.\d{6})\d+(?=Z|[+-]\d\d:\d\d$)", r"\1", text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_recnet_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _validated_event_times(
    start_value: Any, end_value: Any, *, allow_started: bool = False
) -> tuple[datetime | None, datetime | None, int]:
    start = _parse_recnet_datetime(start_value)
    end = _parse_recnet_datetime(end_value)
    if start is None or end is None or end <= start:
        return start, end, 11
    if not allow_started and start <= datetime.now(timezone.utc):
        return start, end, 11
    return start, end, 0


def _event_information(
    event: dict[str, Any], response: dict[str, Any]
) -> dict[str, Any]:
    return {"PlayerEvent": event, "PlayerEventResponse": response}


async def _handle_upcoming_player_events(request: Request, context) -> Response:
    _authenticated_player(request, context)
    now = datetime.now(timezone.utc)
    events = [
        event
        for event in _visible_player_events(context)
        if int(event.get("Accessibility", 0)) == 1
        and (
            (end := _parse_recnet_datetime(event.get("EndTime"))) is not None
            and end > now
        )
    ]
    events.sort(key=lambda event: str(event.get("StartTime") or ""))
    return JSONResponse(events, headers={"Cache-Control": "no-store"})


def _event_room_image(room_id: int, context) -> str:
    coach = _find_coach_room_by_id(room_id, context)
    if coach is not None:
        return str(_serialize_coach_room(coach).get("ImageName") or "")
    ugc = _find_ugc_room(context, room_id=room_id)
    if ugc is not None:
        return str(_serialize_ugc_room(ugc).get("ImageName") or "")
    return ""


async def _handle_create_player_event(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    name = _BASE._str_field(payload, "Name", "name", default="").strip()
    description = _BASE._str_field(payload, "Description", "description", default="").strip()
    start_time = _BASE._str_field(payload, "StartTime", "startTime", default="")
    end_time = _BASE._str_field(payload, "EndTime", "endTime", default="")
    accessibility = _BASE._int_field(payload, "Accessibility", "accessibility", default=1)
    if len(name) < 3:
        return JSONResponse({"PlayerEvent": None, "Result": 14})
    name = _filter_user_text(
        context,
        name,
        policy="censor",
        field_context="event.name",
        player=player,
    )
    description = _filter_user_text(
        context,
        description,
        policy="censor",
        field_context="event.description",
        player=player,
    )
    room = _find_coach_room_by_id(room_id, context) or _find_ugc_room(context, room_id=room_id)
    if room is None:
        return JSONResponse({"PlayerEvent": None, "Result": 4})
    start, end, date_result = _validated_event_times(start_time, end_time)
    if date_result != 0 or start is None or end is None:
        return JSONResponse({"PlayerEvent": None, "Result": date_result})
    if accessibility not in {0, 1, 2}:
        accessibility = 1
    with _EVENT_LOCK:
        events = _global_player_events(context)
        event_id = max((int(item.get("PlayerEventId", 0) or 0) for item in events), default=10_000) + 1
        event = {
            "PlayerEventId": event_id,
            "CreatorPlayerId": _legacy_id_for_player(player),
            "RoomId": room_id,
            "Name": name,
            "Description": description,
            "StartTime": _format_recnet_datetime(start),
            "EndTime": _format_recnet_datetime(end),
            "AttendeeCount": 0,
            "ImageName": _event_room_image(room_id, context),
            "Accessibility": accessibility,
        }
        events.append(event)
        _save_global_player_events(context, events)
    key = _canonical_player_setting_key("player_events", player["player_id"])
    local = _BASE._get_json_setting(context, key, {"Created": [], "Responses": []})
    if not isinstance(local, dict):
        local = {"Created": [], "Responses": []}
    created = local.get("Created") if isinstance(local.get("Created"), list) else []
    created = [item for item in created if not isinstance(item, dict) or int(item.get("PlayerEventId", 0) or 0) != event_id]
    created.append(event)
    local["Created"] = created
    local["Responses"] = local.get("Responses") if isinstance(local.get("Responses"), list) else []
    _BASE._set_json_setting(context, key, local)
    await _send_hub_notification(
        _legacy_id_for_player(player), 80, event, context=context
    )
    return JSONResponse({"PlayerEvent": event, "Result": 0})


async def _handle_get_player_event(event_id: int, request: Request, context) -> Response:
    _authenticated_player(request, context)
    event = _event_by_id(event_id, context)
    if event is None:
        raise HTTPException(status_code=404, detail="Player event not found.")
    return JSONResponse(event)


async def _handle_get_room_by_id_v2(room_id: int, request: Request, context) -> Response:
    _authenticated_player(request, context)
    coach = _find_coach_room_by_id(room_id, context)
    if coach is not None:
        return JSONResponse(_serialize_coach_room(coach))
    dorm = _find_dorm_room_by_room_id(context, room_id)
    if dorm is not None:
        return JSONResponse(_serialize_dorm_room(dorm))
    ugc = _find_ugc_room(context, room_id=room_id)
    if ugc is not None:
        return JSONResponse(_serialize_ugc_room(ugc))
    raise HTTPException(status_code=404, detail="Room not found.")


async def _handle_modify_player_event(event_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    with _EVENT_LOCK:
        events = _global_player_events(context)
        event = next(
            (
                item
                for item in events
                if int(item.get("PlayerEventId", 0) or 0) == event_id
                and not context.is_content_quarantined("player_event", event_id)
            ),
            None,
        )
    if event is None:
        return JSONResponse({"PlayerEvent": None, "Result": 2})
    if int(event.get("CreatorPlayerId", 0) or 0) != _legacy_id_for_player(player):
        return JSONResponse({"PlayerEvent": event, "Result": 8})

    name = _BASE._str_field(payload, "Name", "name", default=str(event.get("Name") or "")).strip()
    description = _BASE._str_field(
        payload, "Description", "description", default=str(event.get("Description") or "")
    ).strip()
    if len(name) < 3:
        return JSONResponse({"PlayerEvent": event, "Result": 14})
    name = _filter_user_text(
        context,
        name,
        policy="censor",
        field_context="event.name",
        player=player,
    )
    description = _filter_user_text(
        context,
        description,
        policy="censor",
        field_context="event.description",
        player=player,
    )
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=int(event.get("RoomId", 0) or 0))
    room = _find_coach_room_by_id(room_id, context) or _find_ugc_room(context, room_id=room_id)
    if room is None:
        return JSONResponse({"PlayerEvent": event, "Result": 4})
    accessibility = _BASE._int_field(
        payload, "Accessibility", "accessibility", default=int(event.get("Accessibility", 1) or 1)
    )
    if accessibility not in {0, 1, 2}:
        accessibility = 1
    start, end, date_result = _validated_event_times(
        _BASE._str_field(
            payload, "StartTime", "startTime", default=str(event.get("StartTime") or "")
        ),
        _BASE._str_field(
            payload, "EndTime", "endTime", default=str(event.get("EndTime") or "")
        ),
        allow_started=True,
    )
    if date_result != 0 or start is None or end is None:
        return JSONResponse({"PlayerEvent": event, "Result": date_result})
    with _EVENT_LOCK:
        # Re-read under the write lock so concurrent responses or edits cannot
        # be overwritten by this request's earlier snapshot.
        events = _global_player_events(context)
        current = next(
            (
                item
                for item in events
                if int(item.get("PlayerEventId", 0) or 0) == event_id
                and not context.is_content_quarantined("player_event", event_id)
            ),
            None,
        )
        if current is None:
            return JSONResponse({"PlayerEvent": None, "Result": 2})
        current.update(
            {
                "RoomId": room_id,
                "Name": name,
                "Description": description,
                "StartTime": _format_recnet_datetime(start),
                "EndTime": _format_recnet_datetime(end),
                "ImageName": _event_room_image(room_id, context),
                "Accessibility": accessibility,
            }
        )
        event = current
        _save_global_player_events(context, events)
    recipients = {
        int(event.get("CreatorPlayerId") or 0),
        *{
            int(item.get("PlayerId") or 0)
            for item in _global_player_event_responses(context)
            if int(item.get("PlayerEventId") or 0) == event_id
        },
    }
    for recipient_id in sorted(value for value in recipients if value > 0):
        await _send_hub_notification(
            recipient_id, 81, event, context=context
        )
    return JSONResponse({"PlayerEvent": event, "Result": 0})


async def _handle_delete_player_event(event_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    events = _global_player_events(context)
    event = next(
        (
            item
            for item in events
            if int(item.get("PlayerEventId", 0) or 0) == event_id
            and not context.is_content_quarantined("player_event", event_id)
        ),
        None,
    )
    if event is None:
        return JSONResponse({"PlayerEvent": None, "Result": 2})
    if int(event.get("CreatorPlayerId", 0) or 0) != _legacy_id_for_player(player):
        return JSONResponse({"PlayerEvent": event, "Result": 8})
    recipients = {
        int(event.get("CreatorPlayerId") or 0),
        *{
            int(item.get("PlayerId") or 0)
            for item in _global_player_event_responses(context)
            if int(item.get("PlayerEventId") or 0) == event_id
        },
    }
    _save_global_player_events(
        context, [item for item in events if int(item.get("PlayerEventId", 0) or 0) != event_id]
    )
    _save_global_player_event_responses(
        context,
        [
            response
            for response in _global_player_event_responses(context)
            if int(response.get("PlayerEventId", 0) or 0) != event_id
        ],
    )
    for recipient_id in sorted(value for value in recipients if value > 0):
        await _send_hub_notification(
            recipient_id,
            82,
            {"PlayerEventId": event_id},
            context=context,
        )
    return JSONResponse({"PlayerEvent": event, "Result": 0})


async def _handle_get_player_event_responses(event_id: int, request: Request, context) -> Response:
    _authenticated_player(request, context)
    if _event_by_id(event_id, context) is None:
        raise HTTPException(status_code=404, detail="Player event not found.")
    responses = [
        response
        for response in _global_player_event_responses(context)
        if int(response.get("PlayerEventId", 0) or 0) == event_id
    ]
    responses.sort(key=lambda response: int(response.get("PlayerEventResponseId", 0) or 0))
    return JSONResponse(responses)


async def _handle_respond_to_player_event(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    event_id = _BASE._int_field(payload, "PlayerEventId", "playerEventId", default=0)
    response_type = _payload_enum(
        payload,
        "Type",
        "type",
        names={"none": -1, "yes": 0, "interested": 1, "no": 2, "pending": 3},
        default=-1,
    )
    player_id = _legacy_id_for_player(player)
    with _EVENT_LOCK:
        event = _event_by_id(event_id, context)
        if event is None:
            return JSONResponse({"Result": 2})
        if response_type not in {0, 1, 2, 3}:
            return JSONResponse({"Result": 7})
        responses = _global_player_event_responses(context)
        existing = next(
            (
                response
                for response in responses
                if int(response.get("PlayerEventId", 0) or 0) == event_id
                and int(response.get("PlayerId", 0) or 0) == player_id
            ),
            None,
        )
        if existing is None:
            response_id = max(
                (int(response.get("PlayerEventResponseId", 0) or 0) for response in responses), default=20_000
            ) + 1
            existing = {
                "PlayerEventResponseId": response_id,
                "PlayerEventId": event_id,
                "PlayerId": player_id,
                "CreatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "Type": response_type,
            }
            responses.append(existing)
        else:
            existing["Type"] = response_type
        _save_global_player_event_responses(context, responses)
        refreshed_event = _refresh_event_attendee_count(event_id, context) or event
    await _send_hub_notification(
        player_id,
        83,
        _event_information(refreshed_event, existing),
        context=context,
    )
    creator_id = int(refreshed_event.get("CreatorPlayerId") or 0)
    if creator_id > 0 and creator_id != player_id:
        await _send_hub_notification(
            creator_id, 81, refreshed_event, context=context
        )
    # CreateModifyPlayerEventResult.Deserialize reads the Result property from
    # a JSON object. A bare integer becomes Int64 and crashes the client cast.
    return JSONResponse({"Result": 0})


async def _handle_delete_player_event_response(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    event_id = _BASE._int_field(payload, "PlayerEventId", "playerEventId", default=0)
    response_type = _payload_enum(
        payload,
        "Type",
        "type",
        names={"none": -1, "yes": 0, "interested": 1, "no": 2, "pending": 3},
        default=-1,
    )
    player_id = _legacy_id_for_player(player)
    with _EVENT_LOCK:
        responses = _global_player_event_responses(context)
        kept = [
            response
            for response in responses
            if not (
                int(response.get("PlayerEventId", 0) or 0) == event_id
                and int(response.get("PlayerId", 0) or 0) == player_id
                and (response_type == -1 or int(response.get("Type", -1)) == response_type)
            )
        ]
        if len(kept) == len(responses):
            return JSONResponse({"Result": 9})
        _save_global_player_event_responses(context, kept)
        refreshed_event = _refresh_event_attendee_count(event_id, context)
    await _send_hub_notification(
        player_id,
        85,
        {"PlayerEventId": event_id},
        context=context,
    )
    if refreshed_event is not None:
        creator_id = int(refreshed_event.get("CreatorPlayerId") or 0)
        if creator_id > 0 and creator_id != player_id:
            await _send_hub_notification(
                creator_id, 81, refreshed_event, context=context
            )
    return JSONResponse({"Result": 0})


async def _handle_bulk_invite_to_player_event(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    event_id = _BASE._int_field(payload, "PlayerEventId", "playerEventId", default=0)
    event = _event_by_id(event_id, context)
    if event is None:
        return JSONResponse({"FailedInvites": [], "Result": 2})
    if int(event.get("CreatorPlayerId", 0) or 0) != _legacy_id_for_player(player):
        return JSONResponse({"FailedInvites": [], "Result": 8})
    invited_ids = payload.get("InvitedPlayerIds", payload.get("invitedPlayerIds", []))
    if not isinstance(invited_ids, list):
        invited_ids = []
    responses = _global_player_event_responses(context)
    failed: list[dict[str, int]] = []
    for raw_id in invited_ids:
        try:
            invited_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if _find_player_by_legacy_id_25april2019(context, invited_id) is None:
            failed.append({"InvitedPlayerId": invited_id, "Result": 3})
            continue
        existing = next(
            (
                response
                for response in responses
                if int(response.get("PlayerEventId", 0) or 0) == event_id
                and int(response.get("PlayerId", 0) or 0) == invited_id
            ),
            None,
        )
        if existing is not None:
            failed.append({"InvitedPlayerId": invited_id, "Result": 10})
            continue
        response_id = max(
            (int(response.get("PlayerEventResponseId", 0) or 0) for response in responses), default=20_000
        ) + 1
        response = {
            "PlayerEventResponseId": response_id,
            "PlayerEventId": event_id,
            "PlayerId": invited_id,
            "CreatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "Type": 3,
        }
        responses.append(response)
        await _send_hub_notification(
            invited_id,
            83,
            _event_information(event, response),
            context=context,
        )
    _save_global_player_event_responses(context, responses)
    return JSONResponse({"FailedInvites": failed, "Result": 16 if failed else 0})


async def _handle_report_player_event(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    event_id = _BASE._int_field(
        payload, "PlayerEventId", "playerEventId", "EventId", "eventId", default=0
    )
    report_category = _BASE._int_field(
        payload, "ReportCategory", "reportCategory", default=0
    )
    details = _BASE._str_field(payload, "Details", "details", default="").strip()
    if _event_by_id(event_id, context) is None:
        raise HTTPException(status_code=404, detail="Player event not found.")
    valid_categories = {-1, 0, 1, 2, 3, 4, 5, 6, 7, 10, 100, 101, 102, 103, 104, 1000}
    if report_category not in valid_categories:
        raise HTTPException(status_code=400, detail="Invalid ReportCategory.")
    _submit_canonical_report(
        reporter=player,
        target_type="player_event",
        target_id=event_id,
        raw_category=report_category,
        canonical_category=PLAYER_REPORT_CATEGORY_MAP.get(report_category, "unknown"),
        category_schema="player_event_reporting_v1",
        details=details[:2000],
        room_id=None,
        game_session_id=None,
        source_endpoint="api/playerevents/v1/report",
        source_payload=payload,
        context=context,
    )
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_current_checklist(request: Request, context) -> Response:
    _authenticated_player(request, context)
    return JSONResponse(_current_checklist_items(context))


DAILY_CHECKLIST_SCHEDULE_KEY = "daily_checklist"
DAILY_CHECKLIST_ANCHOR_UTC = datetime(2019, 4, 25, tzinfo=timezone.utc)


def _daily_checklist_period(context) -> dict[str, Any]:
    configured = context.get_server_setting(
        "daily_objectives",
        DAILY_OBJECTIVES_DEFAULTS,
    )
    objective_sets = (
        configured
        if isinstance(configured, list) and configured
        else DAILY_OBJECTIVES_DEFAULTS
    )
    sequence = [
        {"objectives": objective_set}
        for objective_set in objective_sets
        if isinstance(objective_set, list)
    ]
    if not sequence:
        raise HTTPException(
            status_code=503,
            detail="The canonical daily-objective catalog is unavailable.",
        )
    metadata = {
        "strategy": "deterministic_sequence",
        "sequence": sequence,
    }
    catalog_revision = hashlib.sha256(
        json.dumps(metadata, sort_keys=True).encode("utf-8")
    ).hexdigest()[:24]
    context.ensure_anchored_schedule(
        schedule_key=DAILY_CHECKLIST_SCHEDULE_KEY,
        anchor_utc=DAILY_CHECKLIST_ANCHOR_UTC,
        interval_seconds=24 * 60 * 60,
        catalog_revision=catalog_revision,
        metadata=metadata,
    )
    return context.reconcile_registered_period(
        schedule_key=DAILY_CHECKLIST_SCHEDULE_KEY,
        now_utc=datetime.now(timezone.utc),
    )


def _current_checklist_items(
    context,
    *,
    period: dict[str, Any] | None = None,
) -> list[dict[str, int]]:
    period = period or _daily_checklist_period(context)
    objective_set = period["content"].get("objectives")
    if not isinstance(objective_set, list):
        raise HTTPException(
            status_code=503,
            detail="The current daily-objective snapshot is invalid.",
        )
    return [
        {
            "Order": order,
            "Objective": int(item["type"]),
            "Count": int(item["score"]),
            "CreditAmount": _large_token_award(
                "daily-checklist",
                str(period["period_id"]),
                order,
            ),
        }
        for order, item in enumerate(objective_set)
    ]


async def _handle_complete_checklist_item(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    item_index = _BASE._int_field(payload, "ItemIndex", "itemIndex", default=-1)
    checklist_period = _daily_checklist_period(context)
    checklist = _current_checklist_items(context, period=checklist_period)
    if item_index < 0 or item_index >= len(checklist):
        raise HTTPException(status_code=400, detail="ItemIndex is outside the current checklist.")

    period_id = str(checklist_period["period_id"])
    period_date = str(checklist_period["starts_at_utc"])[:10]
    credit_amount = int(checklist[item_index]["CreditAmount"])
    with _PLAYER_STATE_LOCK:
        with context.db.transaction() as conn:
            state_row = conn.execute(
                """
                SELECT state_json FROM player_version_state
                WHERE player_id = ? AND api_version = ?
                """,
                (player["player_id"], API_VERSION),
            ).fetchone()
            try:
                state = json.loads(state_row["state_json"] or "{}") if state_row else {}
            except Exception:
                state = {}
            if not isinstance(state, dict):
                state = {}
            progress_row = conn.execute(
                """
                SELECT state_json FROM timed_content_player_progress
                WHERE player_id = ? AND schedule_key = ? AND period_id = ?
                """,
                (player["player_id"], DAILY_CHECKLIST_SCHEDULE_KEY, period_id),
            ).fetchone()
            if progress_row is not None:
                try:
                    completion = json.loads(progress_row["state_json"] or "{}")
                except Exception:
                    completion = {}
            else:
                legacy_completion = state.get("checklist_completions")
                completion = (
                    dict(legacy_completion)
                    if isinstance(legacy_completion, dict)
                    and legacy_completion.get("date") == period_date
                    else {}
                )
            indices = {
                int(value)
                for value in completion.get("indices", [])
                if str(value).lstrip("-").isdigit()
            }
            already_completed = item_index in indices
            balances = state.get("storefront_balances")
            if not isinstance(balances, dict):
                balances = {}
            balance = max(0, int(balances.get("2", TOKEN_BALANCE) or 0))
            if not already_completed:
                indices.add(item_index)
                balance += credit_amount
            completion = {"indices": sorted(indices)}
            balances["2"] = balance
            state["storefront_balances"] = balances
            conn.execute(
                """
                UPDATE player_version_state
                SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE player_id = ? AND api_version = ?
                """,
                (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
            )
            conn.execute(
                """
                INSERT INTO timed_content_player_progress(
                    player_id, schedule_key, period_id, state_json,
                    reward_claimed, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 0, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                        strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                ON CONFLICT(player_id, schedule_key, period_id) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    player["player_id"],
                    DAILY_CHECKLIST_SCHEDULE_KEY,
                    period_id,
                    json.dumps(completion, sort_keys=True),
                ),
            )

    platform = int(state.get("platform", 0) or 0)
    reward = {
        "BalanceAddType": 303,
        "BaseAward": credit_amount if not already_completed else 0,
        "BonusAward": 0,
        "RateLimit": len(checklist),
        "CurrentCount": len(indices),
        "Total": credit_amount if not already_completed else 0,
        "Platform": platform,
        "BalanceInGiftBox": False,
    }
    response = {
        "Balance": balance,
        "CurrencyType": 2,
        "Platform": platform,
        "BalanceUpdates": [{"UpdateResponse": 0, "Data": reward}],
    }
    if not already_completed:
        await _send_hub_notification(
            # Message 60 requires the full BalanceUpdateResponseDTO envelope.
            _legacy_id_for_player(player), 60, response, context=context
        )
        await _send_hub_notification(
            _legacy_id_for_player(player),
            61,
            {"Balance": balance, "CurrencyType": 2, "Platform": platform},
            context=context,
        )
    return JSONResponse(response)


async def _handle_objective_progress(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("objective_progress", player["player_id"])
    progress = _BASE._get_json_setting(
        context,
        key,
        {"Objectives": [], "ObjectiveGroups": []},
    )
    if not isinstance(progress, dict):
        progress = {"Objectives": [], "ObjectiveGroups": []}
    objectives = progress.get("Objectives")
    objective_groups = progress.get("ObjectiveGroups")
    if not isinstance(objectives, list):
        objectives = []
    if not isinstance(objective_groups, list):
        objective_groups = []
    serialized = {
        "Objectives": [item for item in objectives if isinstance(item, dict)],
        "ObjectiveGroups": [item for item in objective_groups if isinstance(item, dict)],
    }
    _BASE._set_json_setting(context, key, serialized)
    return JSONResponse(serialized)


async def _handle_update_objective(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    index = _BASE._int_field(payload, "Index", "index", default=-1)
    group = _BASE._int_field(payload, "Group", "group", default=-1)
    if index < 0 or group < 0:
        raise HTTPException(status_code=400, detail="Index and Group are required.")
    try:
        objective = {
            "Index": index,
            "Group": group,
            "Progress": float(payload.get("Progress", payload.get("progress", 0.0)) or 0.0),
            "VisualProgress": float(
                payload.get("VisualProgress", payload.get("visualProgress", 0.0)) or 0.0
            ),
            "IsCompleted": bool(payload.get("IsCompleted", payload.get("isCompleted", False))),
            "IsRewarded": bool(payload.get("IsRewarded", payload.get("isRewarded", False))),
            "IsDirty": False,
        }
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Objective progress must be numeric.") from exc
    key = _canonical_player_setting_key("objective_progress", player["player_id"])
    with _PLAYER_STATE_LOCK:
        progress = _BASE._get_json_setting(
            context, key, {"Objectives": [], "ObjectiveGroups": []}
        )
        if not isinstance(progress, dict):
            progress = {"Objectives": [], "ObjectiveGroups": []}
        objectives = progress.get("Objectives")
        if not isinstance(objectives, list):
            objectives = []
        objectives = [
            item
            for item in objectives
            if not (
                isinstance(item, dict)
                and _BASE._int_field(item, "Index", "index", default=-1) == index
                and _BASE._int_field(item, "Group", "group", default=-1) == group
            )
        ]
        objectives.append(objective)
        objectives.sort(
            key=lambda item: (
                _BASE._int_field(item, "Group", "group", default=0),
                _BASE._int_field(item, "Index", "index", default=0),
            )
        )
        progress["Objectives"] = objectives
        if not isinstance(progress.get("ObjectiveGroups"), list):
            progress["ObjectiveGroups"] = []
        _BASE._set_json_setting(context, key, progress)
    # Objectives.Save uses a non-generic callback and expects an empty success.
    return Response(status_code=204)


async def _handle_complete_objective_group(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    group = _BASE._int_field(payload, "Group", "group", default=-1)
    if group < 0:
        raise HTTPException(status_code=400, detail="Group is required.")
    completed = {
        "Group": group,
        "IsCompleted": True,
        "ClearedAt": _format_recnet_datetime(datetime.now(timezone.utc)),
        "RequiresCompleteOnServer": False,
    }
    key = _canonical_player_setting_key("objective_progress", player["player_id"])
    with _PLAYER_STATE_LOCK:
        progress = _BASE._get_json_setting(
            context, key, {"Objectives": [], "ObjectiveGroups": []}
        )
        if not isinstance(progress, dict):
            progress = {"Objectives": [], "ObjectiveGroups": []}
        groups = progress.get("ObjectiveGroups")
        if not isinstance(groups, list):
            groups = []
        groups = [
            item
            for item in groups
            if not (
                isinstance(item, dict)
                and _BASE._int_field(item, "Group", "group", default=-1) == group
            )
        ]
        groups.append(completed)
        groups.sort(key=lambda item: _BASE._int_field(item, "Group", "group", default=0))
        progress["ObjectiveGroups"] = groups
        if not isinstance(progress.get("Objectives"), list):
            progress["Objectives"] = []
        _BASE._set_json_setting(context, key, progress)
    return JSONResponse(completed)


async def _handle_clear_objective_group(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    group = _BASE._int_field(payload, "Group", "group", default=-1)
    if group < 0:
        raise HTTPException(status_code=400, detail="Group is required.")
    key = _canonical_player_setting_key("objective_progress", player["player_id"])
    progress = _BASE._get_json_setting(context, key, {"Objectives": [], "ObjectiveGroups": []})
    if not isinstance(progress, dict):
        progress = {"Objectives": [], "ObjectiveGroups": []}
    groups = progress.get("ObjectiveGroups") if isinstance(progress.get("ObjectiveGroups"), list) else []
    groups = [
        item for item in groups
        if not isinstance(item, dict) or _BASE._int_field(item, "Group", "group", default=-1) != group
    ]
    progress["ObjectiveGroups"] = groups
    _BASE._set_json_setting(context, key, progress)
    return JSONResponse({
        "Group": group,
        "IsCompleted": False,
        "ClearedAt": "2019-04-25T23:23:01Z",
        "RequiresCompleteOnServer": False,
    })


async def _handle_display_name(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    name = _BASE._str_field(payload, "Name", "name").strip()
    if not name or len(name) > 32:
        raise HTTPException(status_code=400, detail="Name must contain 1 to 32 characters.")
    name = _filter_user_text(
        context,
        name,
        policy="reject_profile",
        field_context="profile.display_name",
        player=player,
    )
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE players SET display_name = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND is_coach = 0
            """,
            (name, player["player_id"]),
        )
    # Push the full profile because this notification replaces cached entries.
    await _broadcast_profile_update(_legacy_id_for_player(player), context)
    return Response(status_code=204)


async def _handle_bio_update(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    bio = _BASE._str_field(payload, "Bio", "bio", default="").strip()
    if len(bio) > 512:
        raise HTTPException(status_code=400, detail="Bio must not exceed 512 characters.")
    bio = _filter_user_text(
        context,
        bio,
        policy="reject_profile",
        field_context="profile.bio",
        player=player,
    )
    state = _player_state(player)
    state["bio"] = bio
    _persist_player_state(player, state, context)
    # Broadcast the full profile so connected clients refresh cached bios.
    await _broadcast_profile_update(_legacy_id_for_player(player), context)
    # ProfileWatchUIFlow expects RecNet.OkResponse, not an empty success body.
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_update_selected_cheer(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    selected = payload.get("CheerCategory", payload.get("cheerCategory"))
    if selected in {"", "null", "None"}:
        selected = None
    if selected is not None:
        try:
            selected = int(selected)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid CheerCategory.") from exc
        if selected not in {0, 10, 20, 30, 40}:
            raise HTTPException(status_code=400, detail="Invalid CheerCategory.")
    state = _player_state(player)
    state["selected_cheer"] = selected
    _persist_player_state(player, state, context)
    legacy_player_id = _legacy_id_for_player(player)
    # Broadcast cheer changes because all clients share the profile cache.
    await _broadcast_profile_update(legacy_player_id, context)
    return Response(status_code=204)


async def _handle_create_cheer(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    target_id = _BASE._int_field(payload, "PlayerIdTo", "playerIdTo", default=0)
    category = _BASE._int_field(payload, "CheerCategory", "cheerCategory", default=-1)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    # Parse multipart booleans explicitly; bool("false") is true in Python.
    anonymous = _bool_value(payload.get("Anonymous", payload.get("anonymous", False)))
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target_id <= 0 or target is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    if target_id == _legacy_id_for_player(player):
        raise HTTPException(status_code=400, detail="A player cannot cheer themselves.")
    if category not in {0, 10, 20, 30, 40}:
        raise HTTPException(status_code=400, detail="Invalid CheerCategory.")
    cheer_fields = {
        0: "CheerGeneral",
        10: "CheerHelpful",
        20: "CheerSportsman",
        30: "CheerGreatHost",
        40: "CheerCreative",
    }
    target_state = _player_state(target)
    counts = target_state.get("cheer_counts")
    if not isinstance(counts, dict):
        counts = {}
    field = cheer_fields[category]
    counts[field] = max(1, int(counts.get(field, 0) or 0)) + 1
    target_state["cheer_counts"] = counts
    # Select the first received cheer so its nonzero badge becomes visible.
    if target_state.get("selected_cheer") is None:
        target_state["selected_cheer"] = category
    _persist_player_state(target, target_state, context)

    # Send the cheer message and refresh the shared profile counters.
    await _create_recnet_message(
        target,
        from_player_id=0 if anonymous else _legacy_id_for_player(player),
        message_type=51 if anonymous else 50,
        data=str(category),
        room_id=room_id,
        context=context,
    )
    await _broadcast_profile_update(target_id, context)
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_register_account(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    # Email/password are compatibility input only.  Parsing proves the request
    # shape is accepted; values are discarded and never copied into state.
    await _BASE._parse_client_payload(request)
    _enforce_private_verified_account(player, context)
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_phone_update(request: Request, context) -> Response:
    _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    number = _BASE._str_field(payload, "Number", "number").strip()
    digits = "".join(character for character in number if character.isdigit())
    if len(digits) < 4 or len(digits) > 15:
        raise HTTPException(status_code=400, detail="A valid phone number is required.")
    # Compatibility-only: acceptance means verified.  Neither the number nor
    # its last four digits are retained in memory, player state, or SQLite.
    return Response(status_code=204)


async def _handle_phone_verify(request: Request, context) -> Response:
    _authenticated_player(request, context)
    # Read and discard the compatibility payload.  Verification is automatic
    # and no code or phone value is ever persisted.
    await _BASE._parse_client_payload(request)
    return Response(status_code=204)


async def _handle_phone_last_four(request: Request, context) -> Response:
    _authenticated_player(request, context)
    return JSONResponse({"PhoneNumber": ""})


def _inventory_rows(player, context):
    with context.db.connection() as conn:
        return conn.execute(
            """
            SELECT item_key, quantity, state_json, created_at
            FROM inventory_items
            WHERE player_id = ? AND quantity > 0
            ORDER BY item_key
            """,
            (player["player_id"],),
        ).fetchall()


async def _handle_get_equipment(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    # Equipment skins require a persisted purchase or challenge award.
    equipment_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in _inventory_rows(player, context):
        try:
            state = json.loads(row["state_json"] or "{}")
        except Exception:
            state = {}
        if not isinstance(state, dict):
            continue
        prefab_name = state.get("PrefabName", state.get("prefab_name"))
        modification_guid = state.get("ModificationGuid", state.get("modification_guid"))
        if prefab_name is None or modification_guid is None:
            continue
        key = (str(prefab_name), str(modification_guid))
        equipment_by_key[key] = {
            "PrefabName": key[0],
            "ModificationGuid": key[1],
            "UnlockedLevel": int(state.get("UnlockedLevel", state.get("unlocked_level", 0)) or 0),
            "PlatformMask": int(state.get("PlatformMask", state.get("platform_mask", -1)) or -1),
            "IsPlatformLocked": bool(state.get("IsPlatformLocked", state.get("is_platform_locked", False))),
            "Favorited": bool(state.get("Favorited", state.get("favorited", False))),
        }
    return JSONResponse(list(equipment_by_key.values()))


async def _handle_update_equipment(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    updates = await _parse_json_list(request, payload_name="Equipment")
    with context.db.transaction() as conn:
        rows = conn.execute(
            "SELECT item_key, state_json FROM inventory_items WHERE player_id = ? AND quantity > 0",
            (player["player_id"],),
        ).fetchall()
        states: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        for row in rows:
            try:
                state = json.loads(row["state_json"] or "{}")
            except Exception:
                continue
            if not isinstance(state, dict):
                continue
            prefab = str(state.get("PrefabName", state.get("prefab_name", "")) or "")
            modification = str(
                state.get("ModificationGuid", state.get("modification_guid", "")) or ""
            )
            if prefab and modification:
                states[(prefab, modification)] = (str(row["item_key"]), state)
        for update in updates:
            prefab = _BASE._str_field(update, "PrefabName", "prefabName")
            modification = _BASE._str_field(update, "ModificationGuid", "modificationGuid")
            record = states.get((prefab, modification))
            if record is None:
                continue
            item_key, state = record
            state["Favorited"] = bool(update.get("Favorited", update.get("favorited", False)))
            state["UnlockedLevel"] = max(
                0,
                _BASE._int_field(
                    update,
                    "UnlockedLevel",
                    "unlockedLevel",
                    default=int(state.get("UnlockedLevel", 0) or 0),
                ),
            )
            conn.execute(
                """
                UPDATE inventory_items
                SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE player_id = ? AND item_key = ?
                """,
                (json.dumps(state, sort_keys=True), player["player_id"], item_key),
            )
    # Equipments.UpdateFavorites posts a raw JSON list and only expects an HTTP
    # success acknowledgement; it does not deserialize a response DTO.
    return Response(status_code=204)


def _serialize_consumable_inventory_entry(
    state: dict[str, Any],
    *,
    quantity: int,
    created_at: str,
) -> dict[str, Any] | None:
    if not isinstance(state, dict):
        return None
    item_id = state.get("Id", state.get("id"))
    item_desc = state.get(
        "ConsumableItem",
        state.get("ConsumableItemDesc", state.get("consumable_item_desc")),
    )
    if item_id is None or item_desc is None:
        return None
    active_duration = state.get(
        "ActiveDurationMinutes",
        state.get("active_duration_minutes"),
    )
    return {
        "Id": int(item_id),
        "ConsumableItemDesc": str(item_desc),
        "PlatformMask": int(state.get("PlatformMask", state.get("platform_mask", -1)) or -1),
        "CreatedAt": str(state.get("CreatedAt") or created_at),
        "Count": int(quantity),
        "InitialCount": int(
            state.get("InitialCount", state.get("initial_count", quantity)) or 0
        ),
        "UnlockedLevel": int(
            state.get("UnlockedLevel", state.get("unlocked_level", 0)) or 0
        ),
        "IsActive": bool(state.get("IsActive", state.get("is_active", False))),
        "ActiveDurationMinutes": (
            int(active_duration) if active_duration is not None else None
        ),
    }


def _settle_realtime_consumable(
    state: dict[str, Any], quantity: int, *, now: datetime | None = None
) -> tuple[int, dict[str, Any]]:
    """Apply elapsed wall-clock minutes to a RealWorldTime consumable once."""
    if int(state.get("LimitType", -1) or -1) != 2 or not _bool_value(state.get("IsActive")):
        return max(0, int(quantity)), state
    raw_started = str(state.get("ActivatedAt") or "").strip()
    if not raw_started:
        return max(0, int(quantity)), state
    try:
        started = datetime.fromisoformat(raw_started.replace("Z", "+00:00"))
    except ValueError:
        state.pop("ActivatedAt", None)
        return max(0, int(quantity)), state
    current = now or datetime.now(timezone.utc)
    elapsed_minutes = max(0, int((current - started.astimezone(timezone.utc)).total_seconds() // 60))
    if elapsed_minutes <= 0:
        return max(0, int(quantity)), state
    quantity = max(0, int(quantity) - elapsed_minutes)
    state["ActiveDurationMinutes"] = max(
        0, int(state.get("ActiveDurationMinutes", 0) or 0)
    ) + elapsed_minutes
    state["ActivatedAt"] = current.isoformat(timespec="seconds").replace("+00:00", "Z")
    if quantity <= 0:
        state["IsActive"] = False
        state.pop("ActivatedAt", None)
    return quantity, state


async def _handle_get_consumables(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    consumables = []
    with context.db.transaction() as conn:
        rows = conn.execute(
            "SELECT item_key, quantity, state_json, created_at FROM inventory_items WHERE player_id = ?",
            (player["player_id"],),
        ).fetchall()
        for row in rows:
            try:
                state = json.loads(row["state_json"] or "{}")
            except Exception:
                state = {}
            if not isinstance(state, dict):
                continue
            quantity, state = _settle_realtime_consumable(state, int(row["quantity"] or 0))
            if quantity <= 0:
                conn.execute(
                    "DELETE FROM inventory_items WHERE player_id = ? AND item_key = ?",
                    (player["player_id"], str(row["item_key"])),
                )
                continue
            conn.execute(
                "UPDATE inventory_items SET quantity = ?, state_json = ? WHERE player_id = ? AND item_key = ?",
                (quantity, json.dumps(state, sort_keys=True), player["player_id"], str(row["item_key"])),
            )
            consumable = _serialize_consumable_inventory_entry(
                state,
                quantity=quantity,
                created_at=str(row["created_at"]),
            )
            if consumable is not None:
                consumables.append(consumable)
    return JSONResponse(consumables)


async def _handle_consume_consumable(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    item_id = _BASE._int_field(payload, "Id", "id", default=0)
    delta_count = _BASE._int_field(payload, "DeltaCount", "deltaCount", default=0)
    if item_id <= 0 or delta_count == 0:
        raise HTTPException(status_code=400, detail="Id and a non-zero DeltaCount are required.")
    removed_consumable: dict[str, Any] | None = None
    with context.db.transaction() as conn:
        rows = conn.execute(
            "SELECT item_key, quantity, state_json, created_at FROM inventory_items WHERE player_id = ?",
            (player["player_id"],),
        ).fetchall()
        match = None
        for row in rows:
            try:
                state = json.loads(row["state_json"] or "{}")
            except Exception:
                continue
            if isinstance(state, dict) and int(state.get("Id", state.get("id", 0)) or 0) == item_id:
                match = row
                break
        if match is None:
            raise HTTPException(status_code=404, detail="Consumable not found.")
        # The request is an absolute amount used; acknowledge repeats and clamp at zero.
        new_count = max(0, int(match["quantity"] or 0) - abs(delta_count))
        try:
            match_state = json.loads(match["state_json"] or "{}")
        except Exception:
            match_state = {}
        if new_count <= 0:
            removed_consumable = _serialize_consumable_inventory_entry(
                match_state,
                quantity=0,
                created_at=str(match["created_at"]),
            )
            conn.execute(
                "DELETE FROM inventory_items WHERE player_id = ? AND item_key = ?",
                (player["player_id"], str(match["item_key"])),
            )
        else:
            conn.execute(
                "UPDATE inventory_items SET quantity = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE player_id = ? AND item_key = ?",
                (new_count, player["player_id"], str(match["item_key"])),
            )
    if removed_consumable is not None:
        # Notification 71 deserializes a full Consumable then raises Removed(Id).
        await _send_hub_notification(
            _legacy_id_for_player(player), 71, removed_consumable, context=context
        )
    # UseConsumable expects an empty acknowledgement body.
    return Response(status_code=204)


async def _handle_update_active_consumable(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    item_id = _BASE._int_field(payload, "Id", "id", default=0)
    is_active = _bool_value(payload.get("IsActive", payload.get("isActive", False)))
    if item_id <= 0:
        raise HTTPException(status_code=400, detail="Id is required.")
    removed_consumable: dict[str, Any] | None = None
    with context.db.transaction() as conn:
        rows = conn.execute(
            "SELECT item_key, quantity, state_json, created_at FROM inventory_items WHERE player_id = ?",
            (player["player_id"],),
        ).fetchall()
        match = None
        match_state: dict[str, Any] | None = None
        for row in rows:
            try:
                state = json.loads(row["state_json"] or "{}")
            except Exception:
                continue
            if isinstance(state, dict) and int(state.get("Id", state.get("id", 0)) or 0) == item_id:
                match = row
                match_state = state
                break
        if match is None or match_state is None:
            raise HTTPException(status_code=404, detail="Consumable not found.")
        quantity, match_state = _settle_realtime_consumable(
            match_state, int(match["quantity"] or 0)
        )
        # An empty stack cannot remain active. Still acknowledge the request
        # so the client's optimistic state does not oscillate on retries.
        match_state["IsActive"] = bool(is_active and quantity > 0)
        if match_state["IsActive"] and int(match_state.get("LimitType", -1) or -1) == 2:
            match_state["ActivatedAt"] = datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            ).replace("+00:00", "Z")
            match_state.setdefault("ActiveDurationMinutes", 0)
        elif not match_state["IsActive"]:
            match_state.pop("ActivatedAt", None)
        if quantity <= 0:
            removed_consumable = _serialize_consumable_inventory_entry(
                match_state,
                quantity=0,
                created_at=str(match["created_at"]),
            )
            conn.execute(
                "DELETE FROM inventory_items WHERE player_id = ? AND item_key = ?",
                (player["player_id"], str(match["item_key"])),
            )
        else:
            conn.execute(
                """
                UPDATE inventory_items
                SET quantity = ?, state_json = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE player_id = ? AND item_key = ?
                """,
                (
                    quantity,
                    json.dumps(match_state, sort_keys=True),
                    player["player_id"],
                    str(match["item_key"]),
                ),
            )
    if removed_consumable is not None:
        await _send_hub_notification(
            _legacy_id_for_player(player), 71, removed_consumable, context=context
        )
    return Response(status_code=204)


def _player_state(player) -> dict[str, Any]:
    if hasattr(player, "get") and isinstance(player.get("state"), dict):
        return dict(player["state"])
    raw_state = player.get("state_json") if hasattr(player, "get") else player["state_json"]
    try:
        state = json.loads(raw_state or "{}")
    except Exception:
        state = {}
    return state if isinstance(state, dict) else {}


def _ensure_dorm_room(player, context) -> dict[str, Any]:
    player_id = str(player["player_id"])
    state = _player_state(player)
    creator_player_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
    if creator_player_id <= 0:
        raise HTTPException(status_code=409, detail="Player has no 2019 profile ID.")

    with context.db.transaction() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM rooms
            WHERE owner_player_id = ? AND lower(name) = 'dormroom'
            ORDER BY created_at
            LIMIT 1
            """,
            (player_id,),
        ).fetchone()

        if row is None:
            canonical_room_id = f"dorm-{secrets.token_hex(16)}"
            conn.execute(
                """
                INSERT INTO rooms(
                    room_id, owner_player_id, name, is_official, metadata_json,
                    created_at, updated_at
                )
                VALUES (?, ?, 'DormRoom', 0, '{}',
                        strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                        strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                """,
                (canonical_room_id, player_id),
            )
            row = conn.execute("SELECT * FROM rooms WHERE room_id = ?", (canonical_room_id,)).fetchone()

        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except Exception:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        versions = metadata.setdefault("versions", {})
        if not isinstance(versions, dict):
            versions = {}
            metadata["versions"] = versions
        version = versions.get(API_VERSION)
        if not isinstance(version, dict):
            version = {}
            versions[API_VERSION] = version

        existing_ids = {"room_id": [], "scene_id": [], "game_session_id": []}
        for candidate in conn.execute("SELECT metadata_json FROM rooms").fetchall():
            try:
                candidate_metadata = json.loads(candidate["metadata_json"] or "{}")
                candidate_version = candidate_metadata.get("versions", {}).get(API_VERSION, {})
            except Exception:
                candidate_version = {}
            if not isinstance(candidate_version, dict):
                continue
            for key in existing_ids:
                try:
                    value = int(candidate_version.get(key) or 0)
                except (TypeError, ValueError):
                    value = 0
                if value > 0:
                    existing_ids[key].append(value)

        changed = False
        for key in existing_ids:
            try:
                value = int(version.get(key) or 0)
            except (TypeError, ValueError):
                value = 0
            if value <= 0:
                version[key] = max(existing_ids[key], default=0) + 1
                existing_ids[key].append(int(version[key]))
                changed = True

        exact_defaults = {
            "room_scene_location_id": "76d98498-60a1-430c-ab76-b54a29b7a163",
            "room_scene_name": "Home",
            "data_blob_name": "",
            "photon_region_id": "us",
            "photon_room_id": f"dorm-{int(version['game_session_id'])}",
            "max_players": 4,
        }
        for key, value in exact_defaults.items():
            if key not in version:
                version[key] = value
                changed = True
        for key, value in {
            "room_kind": "personal_dorm",
            "description": "Your private room",
            "image_name": "",
        }.items():
            if key not in metadata:
                metadata[key] = value
                changed = True

        if changed:
            conn.execute(
                """
                UPDATE rooms
                SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE room_id = ?
                """,
                (json.dumps(metadata, sort_keys=True), row["room_id"]),
            )
            row = conn.execute("SELECT * FROM rooms WHERE room_id = ?", (row["room_id"],)).fetchone()

    return {
        "row": {key: row[key] for key in row.keys()},
        "metadata": metadata,
        "version": version,
        "creator_player_id": creator_player_id,
    }


def _find_dorm_room_by_room_id(context, room_id: int) -> dict[str, Any] | None:
    """Resolve any player's personal DormRoom, not only the caller's room."""
    with context.db.connection() as conn:
        rows = conn.execute(
            "SELECT * FROM rooms WHERE is_official = 0 AND lower(name) = 'dormroom'"
        ).fetchall()
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except Exception:
                continue
            if not isinstance(metadata, dict) or metadata.get("room_kind") != "personal_dorm":
                continue
            version = metadata.get("versions", {}).get(API_VERSION, {})
            if not isinstance(version, dict) or int(version.get("room_id") or 0) != room_id:
                continue
            owner = conn.execute(
                """
                SELECT pvs.state_json
                FROM player_version_state AS pvs
                WHERE pvs.player_id = ? AND pvs.api_version = ?
                """,
                (row["owner_player_id"], API_VERSION),
            ).fetchone()
            if owner is None:
                return None
            try:
                owner_state = json.loads(owner["state_json"] or "{}")
            except Exception:
                owner_state = {}
            creator_player_id = int(
                owner_state.get("legacy_player_id") or owner_state.get("recnet_id") or 0
            )
            if creator_player_id <= 0:
                return None
            return {
                "row": {key: row[key] for key in row.keys()},
                "metadata": metadata,
                "version": version,
                "creator_player_id": creator_player_id,
            }
    return None


def _serialize_dorm_room(dorm: dict[str, Any]) -> dict[str, Any]:
    metadata = dorm["metadata"]
    version = dorm["version"]
    return {
        "RoomId": int(version["room_id"]),
        "Name": "DormRoom",
        "Description": str(metadata.get("description") or ""),
        "CreatorPlayerId": int(dorm["creator_player_id"]),
        "ImageName": str(metadata.get("image_name") or ""),
        "State": 0,
        "Accessibility": 0,
        "SupportsLevelVoting": False,
        "IsAGRoom": False,
        "IsDormRoom": True,
        "CloningAllowed": False,
        "SupportsVRLow": True,
        "SupportsScreens": True,
        "SupportsWalkVR": True,
        "SupportsTeleportVR": True,
        "AllowsJuniors": True,
        "DisableMicAutoMute": False,
    }


def _serialize_dorm_scene(dorm: dict[str, Any]) -> dict[str, Any]:
    row = dorm["row"]
    version = dorm["version"]
    return {
        "RoomSceneId": int(version["scene_id"]),
        "RoomId": int(version["room_id"]),
        "RoomSceneLocationId": str(version["room_scene_location_id"]),
        "Name": str(version["room_scene_name"]),
        "IsSandbox": False,
        "DataBlobName": str(version.get("data_blob_name") or ""),
        "MaxPlayers": int(version.get("max_players") or 4),
        "CanMatchmakeInto": False,
        "DataModifiedAt": str(row["updated_at"]),
    }


def _serialize_dorm_details(
    dorm: dict[str, Any], *, local_player_id: int | None = None
) -> dict[str, Any]:
    creator_id = int(dorm["creator_player_id"])
    return {
        "Room": _serialize_dorm_room(dorm),
        "Scenes": [_serialize_dorm_scene(dorm)],
        "CoOwners": [],
        "InvitedCoOwners": [],
        "Hosts": [],
        "InvitedHosts": [],
    # The watch gates Save/Restore on RoomRole (Guest=0 through Creator=3).
        "LocalPlayerRole": (
            3 if local_player_id is not None and int(local_player_id) == creator_id else 0
        ),
        "CheerCount": 0,
        "FavoriteCount": 0,
        "VisitCount": 0,
        # Rec Center return spawns match the #dormroom tag.
        "Tags": [{"Tag": "dormroom", "Type": 2}],
    }


def _serialize_dorm_game_session(dorm: dict[str, Any]) -> dict[str, Any]:
    version = dorm["version"]
    return {
        "GameSessionId": int(version["game_session_id"]),
        "PhotonRegionId": str(version["photon_region_id"]),
        "PhotonRoomId": str(version["photon_room_id"]),
        "Name": "DormRoom",
        "RoomId": int(version["room_id"]),
        "RoomSceneId": int(version["scene_id"]),
        "RoomSceneLocationId": str(version["room_scene_location_id"]),
        "IsSandbox": False,
        "DataBlobName": str(version.get("data_blob_name") or ""),
        "PlayerEventId": None,
        "Private": True,
        "GameInProgress": False,
        "MaxCapacity": int(version.get("max_players") or 4),
        "IsFull": False,
    }


def _coach_asset_indexes() -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    room_ids: dict[str, int] = {}
    scene_ids: dict[tuple[str, str], int] = {}
    next_scene_id = 10_001
    for room_index, room in enumerate(BUILD_COACH_ROOMS, start=1):
        room_ids[str(room["n"]).casefold()] = 1_000 + room_index
        for scene in room["x"]:
            scene_ids[(str(room["n"]).casefold(), str(scene["n"]).casefold())] = next_scene_id
            next_scene_id += 1
    return room_ids, scene_ids


BUILD_COACH_ROOM_IDS, BUILD_COACH_SCENE_IDS = _coach_asset_indexes()


def _ensure_coach_rooms(context) -> None:
    """Persist exact released Coach room metadata without inventing asset IDs."""
    with context.db.transaction() as conn:
        coach_exists = conn.execute(
            "SELECT 1 FROM players WHERE player_id = ?", (COACH_PLAYER_UUID,)
        ).fetchone()
        owner_id = COACH_PLAYER_UUID if coach_exists is not None else None
        conn.execute(
            "DELETE FROM rooms WHERE room_id = ? AND created_by_system = 1",
            (f"{API_VERSION}-coach-30040e05-b7b9-9f44-eb08-b9f154d2ecfc",),
        )
        for room in BUILD_COACH_ROOMS:
            name = str(room["n"])
            room_id = BUILD_COACH_ROOM_IDS[name.casefold()]
            canonical_room_id = f"{API_VERSION}-coach-{room['g']}"
            existing = conn.execute(
                "SELECT metadata_json FROM rooms WHERE room_id = ?", (canonical_room_id,)
            ).fetchone()
            try:
                existing_metadata = json.loads(existing["metadata_json"] or "{}") if existing else {}
            except Exception:
                existing_metadata = {}
            if not isinstance(existing_metadata, dict):
                existing_metadata = {}
            metadata = {
                "room_kind": "coach_original",
                "description": str(room.get("d") or ""),
                "image_name": COACH_ROOM_IMAGE_TEXTURES.get(name, ""),
                "asset_replication_id": str(room["g"]),
                "visit_count": int(existing_metadata.get("visit_count", 0) or 0),
                "game_session_count": int(existing_metadata.get("game_session_count", 0) or 0),
                "cheer_count": int(existing_metadata.get("cheer_count", 0) or 0),
                "favorite_count": int(existing_metadata.get("favorite_count", 0) or 0),
                "versions": {
                    API_VERSION: {
                        "room_id": room_id,
                        "asset": room,
                    }
                },
            }
            conn.execute(
                """
                INSERT INTO rooms(
                    room_id, owner_player_id, name, is_official, metadata_json,
                    created_at, updated_at, creator_player_id,
                    is_coach_only_edit, created_by_system
                ) VALUES (?, ?, ?, 1, ?,
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, 1, 1)
                ON CONFLICT(room_id) DO UPDATE SET
                    owner_player_id = excluded.owner_player_id,
                    name = excluded.name,
                    is_official = 1,
                    metadata_json = excluded.metadata_json,
                    creator_player_id = excluded.creator_player_id,
                    is_coach_only_edit = 1,
                    created_by_system = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                """,
                (canonical_room_id, owner_id, name, json.dumps(metadata, sort_keys=True), owner_id),
            )


def _coach_room_record(room: dict[str, Any]) -> dict[str, Any]:
    return {
        "asset": room,
        "room_id": BUILD_COACH_ROOM_IDS[str(room["n"]).casefold()],
        "canonical_room_id": f"{API_VERSION}-coach-{room['g']}",
        "updated_at": "2019-04-25T23:23:01Z",
    }


def _find_coach_room_by_name(name: str, context) -> dict[str, Any] | None:
    _ensure_coach_rooms(context)
    folded = name.casefold()
    for room in BUILD_COACH_ROOMS:
        if str(room["n"]).casefold() == folded:
            return _coach_room_record(room)
    return None


def _find_coach_room_by_id(room_id: int, context) -> dict[str, Any] | None:
    _ensure_coach_rooms(context)
    for room in BUILD_COACH_ROOMS:
        record = _coach_room_record(room)
        if record["room_id"] == room_id:
            return record
    return None


def _coach_room_tag_names(record: dict[str, Any]) -> list[str]:
    """Return the released discovery/arrival tags used by Rec Center doors."""
    room_name = str(record["asset"]["n"]).casefold()
    tags = ["recroomoriginal"]
    if room_name == "dormroom":
        tags.append("dormroom")
    if room_name in {
        "paintball", "paintballvr", "lasertag", "lasertagcyberjunk",
        "recroyalesquads", "recroyalesolos",
    }:
        tags.append("pvp")
    if room_name in {
        "goldentrophy", "theriseofjumbotron", "crimsoncauldron",
        "isleoflostskulls", "crescendo",
    }:
        tags.append("quest")
    if room_name in {
        "3dcharades", "discgolflake", "discgolfpropulsion", "dodgeball",
        "dodgeballvr", "paddleball", "soccer", "bowling",
    }:
        tags.append("sport")
    return tags


def _serialize_coach_room(record: dict[str, Any]) -> dict[str, Any]:
    room = record["asset"]
    return {
        "RoomId": int(record["room_id"]),
        "Name": str(room["n"]),
        "Description": str(room.get("d") or ""),
        "CreatorPlayerId": 1,
        "ImageName": COACH_ROOM_IMAGE_TEXTURES.get(str(room["n"]), ""),
        "State": 0,
        "Accessibility": int(room.get("a", 2)),
        "SupportsLevelVoting": False,
        "IsAGRoom": True,
        "IsDormRoom": str(room["n"]).casefold() == "dormroom",
        # Every built-in Coach room may be copied into an independently owned
        # UGC room. This does not grant permission to modify the source room.
        "CloningAllowed": True,
        "SupportsVRLow": True,
        "SupportsScreens": bool(room.get("s", False)),
        "SupportsWalkVR": bool(room.get("w", False)),
        "SupportsTeleportVR": bool(room.get("t", False)),
        "AllowsJuniors": bool(room.get("j", False)),
        "DisableMicAutoMute": bool(room.get("m", False)),
    }


def _coach_scene_max_players(record: dict[str, Any], scene: dict[str, Any]) -> int:
        # Rec Center is intentionally overridden to a 40-player capacity.
    if str(record["asset"]["n"]).casefold() == "reccenter":
        return 40
    return max(1, int(scene.get("c", 1) or 1))


def _serialize_coach_scene(record: dict[str, Any], scene: dict[str, Any]) -> dict[str, Any]:
    room_name = str(record["asset"]["n"])
    scene_name = str(scene["n"])
    scene_id = BUILD_COACH_SCENE_IDS[(room_name.casefold(), scene_name.casefold())]
    data_blob_name = ""
    with record["context"].db.connection() as conn:
        row = conn.execute(
            """
            SELECT blob_name, updated_at FROM room_data_blobs
            WHERE room_id = ? ORDER BY updated_at DESC LIMIT 1
            """,
            (f"{record['canonical_room_id']}:{scene_id}",),
        ).fetchone()
    modified_at = record["updated_at"]
    if row is not None:
        data_blob_name = str(row["blob_name"])
        modified_at = str(row["updated_at"])
    return {
        "RoomSceneId": scene_id,
        "RoomId": int(record["room_id"]),
        "RoomSceneLocationId": str(scene["l"]),
        "Name": scene_name,
        "IsSandbox": bool(scene.get("b", False)),
        "DataBlobName": data_blob_name,
        "MaxPlayers": _coach_scene_max_players(record, scene),
        "CanMatchmakeInto": bool(scene.get("q", False)),
        "DataModifiedAt": modified_at,
    }


def _serialize_coach_details(
    record: dict[str, Any], context, *, local_player_id: int | None = None
) -> dict[str, Any]:
    record = {**record, "context": context}
    # Grant the visiting player Coach-room Maker Pen permissions only.
    coowners = [int(local_player_id)] if local_player_id and local_player_id > 0 else []
    hosts: list[int] = []
    with context.db.connection() as conn:
        row = conn.execute(
            "SELECT metadata_json FROM rooms WHERE room_id = ?", (record["canonical_room_id"],)
        ).fetchone()
    try:
        metrics = json.loads(row["metadata_json"] or "{}") if row else {}
    except Exception:
        metrics = {}
    if not isinstance(metrics, dict):
        metrics = {}
    room_payload = _serialize_coach_room(record)
    local_role = 0
    if local_player_id is not None:
        if int(local_player_id) == int(room_payload["CreatorPlayerId"]):
            local_role = 3
        elif int(local_player_id) in coowners:
            local_role = 2
        elif int(local_player_id) in hosts:
            local_role = 1
    return {
        "Room": room_payload,
        "Scenes": [_serialize_coach_scene(record, scene) for scene in record["asset"]["x"]],
        "CoOwners": coowners,
        "InvitedCoOwners": [],
        "Hosts": hosts,
        "InvitedHosts": [],
        "LocalPlayerRole": local_role,
        "CheerCount": int(metrics.get("cheer_count", 0) or 0),
        "FavoriteCount": int(metrics.get("favorite_count", 0) or 0),
        "VisitCount": int(metrics.get("visit_count", 0) or 0),
        "Tags": [
            {"Tag": tag, "Type": 2}
            for tag in _coach_room_tag_names(record)
        ],
    }


def _ugc_record_from_row(row) -> dict[str, Any] | None:
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except Exception:
        return None
    if not isinstance(metadata, dict) or metadata.get("room_kind") != "user_created":
        return None
    version = metadata.get("versions", {}).get(API_VERSION, {})
    if not isinstance(version, dict) or int(version.get("room_id") or 0) <= 0:
        return None
    return {
        "row": {key: row[key] for key in row.keys()},
        "metadata": metadata,
        "version": version,
        "creator_player_id": int(metadata.get("creator_legacy_id") or 0),
    }


def _all_ugc_records(
    context,
    *,
    public_only: bool = False,
    include_quarantined: bool = False,
) -> list[dict[str, Any]]:
    with context.db.connection() as conn:
        rows = conn.execute("SELECT * FROM rooms WHERE is_official = 0 ORDER BY created_at").fetchall()
    records = []
    for row in rows:
        record = _ugc_record_from_row(row)
        if record is None:
            continue
        if (
            not include_quarantined
            and context.is_content_quarantined(
                "room",
                int(record["version"]["room_id"]),
            )
        ):
            continue
        if public_only and int(record["metadata"].get("accessibility", 0) or 0) != 1:
            continue
        records.append(record)
    return records


def _find_ugc_room(context, *, room_id: int | None = None, name: str | None = None) -> dict[str, Any] | None:
    for record in _all_ugc_records(context):
        if room_id is not None and int(record["version"]["room_id"]) == room_id:
            return record
        if name is not None and str(record["row"]["name"]).casefold() == name.casefold():
            return record
    return None


def _find_ugc_room_by_scene_id(context, scene_id: int) -> dict[str, Any] | None:
    return next(
        (
            record
            for record in _all_ugc_records(context)
            if int(record["version"]["scene_id"]) == scene_id
        ),
        None,
    )


def _serialize_ugc_room(record: dict[str, Any]) -> dict[str, Any]:
    metadata = record["metadata"]
    return {
        "RoomId": int(record["version"]["room_id"]),
        "Name": str(record["row"]["name"]),
        "Description": str(metadata.get("description") or ""),
        "CreatorPlayerId": int(record["creator_player_id"]),
        "ImageName": str(metadata.get("image_name") or ""),
        "State": 0,
        "Accessibility": int(metadata.get("accessibility", 0) or 0),
        "SupportsLevelVoting": True,
        "IsAGRoom": False,
        "IsDormRoom": False,
        "CloningAllowed": bool(metadata.get("cloning_allowed", True)),
        "SupportsVRLow": True,
        "SupportsScreens": bool(metadata.get("supports_screens", True)),
        "SupportsWalkVR": bool(metadata.get("supports_walk_vr", True)),
        "SupportsTeleportVR": bool(metadata.get("supports_teleport_vr", True)),
        "AllowsJuniors": bool(metadata.get("allows_juniors", True)),
        "DisableMicAutoMute": bool(metadata.get("disable_mic_auto_mute", False)),
    }


def _serialize_ugc_scene(record: dict[str, Any]) -> dict[str, Any]:
    version = record["version"]
    return {
        "RoomSceneId": int(version["scene_id"]),
        "RoomId": int(version["room_id"]),
        "RoomSceneLocationId": str(version["room_scene_location_id"]),
        "Name": str(version.get("room_scene_name") or "Home"),
        "IsSandbox": True,
        "DataBlobName": str(version.get("data_blob_name") or ""),
        "MaxPlayers": int(version.get("max_players") or 20),
        "CanMatchmakeInto": bool(version.get("can_matchmake_into", True)),
        "DataModifiedAt": str(record["row"]["updated_at"]),
    }


def _infer_room_auto_tags(record: dict[str, Any]) -> list[str]:
    """Infer server-owned discovery tags from room metadata."""
    metadata = record["metadata"]
    haystack = " ".join(
        (
            str(record["row"]["name"] or ""),
            str(metadata.get("description") or ""),
            str(record["version"].get("room_scene_name") or ""),
        )
    ).casefold()
    rules = (
        ("pvp", ("pvp", "battle", "combat", "arena", "paintball", "versus")),
        ("simulator", ("simulator", "simulation", " tycoon", "sim ")),
        ("parkour", ("parkour", "obby", "obstacle course")),
        ("hangout", ("hangout", "hang out", "chill", "social", "lounge")),
        ("quest", ("quest", "adventure", "dungeon")),
        ("horror", ("horror", "scary", "haunted")),
        ("roleplay", ("roleplay", "role play", " rp ")),
        ("sports", ("sport", "soccer", "basketball", "dodgeball", "golf")),
        ("art", ("art", "draw", "gallery", "museum")),
        ("sandbox", ("sandbox", "maker", "build", "create")),
    )
    inferred = [tag for tag, terms in rules if any(term in haystack for term in terms)]
    if not inferred:
        inferred.append("community")
    return inferred[:3]


def _serialize_ugc_details(
    record: dict[str, Any], *, local_player_id: int | None = None
) -> dict[str, Any]:
    metadata = record["metadata"]
    # CreatorPlayerId alone owns a clone; do not duplicate it in CoOwners.
    creator_id = int(record["creator_player_id"])
    auto_tags = [
        str(tag).strip().lstrip("#")
        for tag in metadata.get("auto_tags", [])
        if isinstance(tag, str) and str(tag).strip().lstrip("#")
    ]
    if not auto_tags:
        auto_tags = _infer_room_auto_tags(record)
    player_tags = [
        str(tag).strip().lstrip("#")
        for tag in metadata.get("custom_tags", [])
        if isinstance(tag, str) and str(tag).strip().lstrip("#")
    ]
    auto_tag_keys = {tag.casefold() for tag in auto_tags}
    player_tags = [tag for tag in player_tags if tag.casefold() not in auto_tag_keys]
    if not auto_tags and not player_tags:
        for tag in metadata.get("tags", []) if isinstance(metadata.get("tags"), list) else []:
            normalized = str(tag).strip().lstrip("#")
            if normalized:
                player_tags.append(normalized)
    role_lists: dict[str, list[int]] = {}
    for key in (
        "co_owner_ids",
        "invited_co_owner_ids",
        "host_ids",
        "invited_host_ids",
    ):
        raw_values = metadata.get(key, [])
        role_lists[key] = sorted(
            {
                int(value)
                for value in raw_values if isinstance(raw_values, list)
                and str(value).lstrip("-").isdigit()
                and int(value) > 0
                and int(value) != creator_id
            }
        )
    local_role = 0
    if local_player_id is not None:
        if int(local_player_id) == creator_id:
            local_role = 3
        elif int(local_player_id) in role_lists["co_owner_ids"]:
            local_role = 2
        elif int(local_player_id) in role_lists["host_ids"]:
            local_role = 1
    return {
        "Room": _serialize_ugc_room(record),
        "Scenes": [_serialize_ugc_scene(record)],
        "CoOwners": role_lists["co_owner_ids"],
        "InvitedCoOwners": role_lists["invited_co_owner_ids"],
        "Hosts": role_lists["host_ids"],
        "InvitedHosts": role_lists["invited_host_ids"],
        "LocalPlayerRole": local_role,
        "CheerCount": int(metadata.get("cheer_count", 0) or 0),
        "FavoriteCount": int(metadata.get("favorite_count", 0) or 0),
        "VisitCount": int(metadata.get("visit_count", 0) or 0),
        "Tags": [
            *({"Tag": tag, "Type": 1} for tag in auto_tags),
            *({"Tag": tag, "Type": 0} for tag in player_tags),
        ],
    }


def _player_can_edit_ugc(record: dict[str, Any], player) -> bool:
    if str(record["row"]["owner_player_id"]) == str(player["player_id"]):
        return True
    local_id = _legacy_id_for_player(player)
    return local_id > 0 and local_id in {
        int(value) for value in record["metadata"].get("co_owner_ids", [])
        if str(value).lstrip("-").isdigit()
    }


def _next_ugc_ids(conn) -> tuple[int, int, int]:
    room_ids: list[int] = list(BUILD_COACH_ROOM_IDS.values())
    scene_ids: list[int] = list(BUILD_COACH_SCENE_IDS.values())
    session_ids: list[int] = [99_999]
    for row in conn.execute("SELECT metadata_json FROM rooms").fetchall():
        try:
            version = json.loads(row["metadata_json"] or "{}").get("versions", {}).get(API_VERSION, {})
        except Exception:
            continue
        if not isinstance(version, dict):
            continue
        for values, key in ((room_ids, "room_id"), (scene_ids, "scene_id"), (session_ids, "game_session_id")):
            try:
                value = int(version.get(key) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                values.append(value)
    return max(room_ids, default=20_000) + 1, max(scene_ids, default=30_000) + 1, max(session_ids) + 1


async def _handle_clone_room(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    source_room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    name = _BASE._str_field(payload, "Name", "name").strip()
    if not name or len(name) > 32:
        return JSONResponse({"Result": 12, "RoomDetails": None})
    name = _filter_user_text(
        context,
        name,
        policy="censor",
        field_context="room.name",
        player=player,
    )
    source_ugc = None
    source = _find_coach_room_by_id(source_room_id, context)
    if source is not None and source["asset"]["x"]:
        location_id = str(source["asset"]["x"][0]["l"])
    else:
        source_ugc = _find_ugc_room(context, room_id=source_room_id)
        location_id = (
            str(source_ugc["version"]["room_scene_location_id"])
            if source_ugc is not None
            else "76d98498-60a1-430c-ab76-b54a29b7a163"
        )
    source_blob_name = (
        str(source_ugc["version"].get("data_blob_name") or "")
        if source_ugc is not None
        else ""
    )
    with context.db.transaction() as conn:
        duplicate = conn.execute("SELECT 1 FROM rooms WHERE lower(name) = lower(?)", (name,)).fetchone()
        if duplicate is not None:
            return JSONResponse({"Result": 10, "RoomDetails": None})
        room_id, scene_id, game_session_id = _next_ugc_ids(conn)
        source_blob = (
            conn.execute(
                """
                SELECT data, image_list_json
                FROM room_data_blobs
                WHERE blob_name = ?
                """,
                (source_blob_name,),
            ).fetchone()
            if source_blob_name
            else None
        )
        cloned_blob_name = (
            f"{API_VERSION}-{scene_id}-{secrets.token_hex(12)}.room"
            if source_blob is not None
            else ""
        )
        metadata = {
            "room_kind": "user_created",
            "creator_legacy_id": _legacy_id_for_player(player),
            "description": "",
            "image_name": "",
            "accessibility": 0,
            "cloning_allowed": True,
            "supports_screens": True,
            "supports_walk_vr": True,
            "supports_teleport_vr": True,
            "allows_juniors": True,
            "tags": ["#community"],
            "custom_tags": ["community"],
            "auto_tags": [],
            "visit_count": 0,
            "game_session_count": 0,
            "versions": {API_VERSION: {
                "room_id": room_id,
                "scene_id": scene_id,
                "game_session_id": game_session_id,
                "room_scene_location_id": location_id,
                "room_scene_name": "Home",
                "data_blob_name": cloned_blob_name,
                "max_players": 20,
            }},
        }
        canonical_id = f"{API_VERSION}-ugc-{secrets.token_hex(16)}"
        conn.execute(
            """
            INSERT INTO rooms(room_id, owner_player_id, name, is_official, metadata_json,
                              created_at, updated_at, creator_player_id, is_coach_only_edit, created_by_system)
            VALUES (?, ?, ?, 0, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, 0, 0)
            """,
            (canonical_id, player["player_id"], name, json.dumps(metadata, sort_keys=True), player["player_id"]),
        )
        if source_blob is not None:
        # Preserve immutable room history under a new clone-owned row.
            conn.execute(
                """
                INSERT INTO room_data_blobs(
                    blob_name, room_id, owner_player_id, data, image_list_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?,
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                """,
                (
                    cloned_blob_name,
                    f"{canonical_id}:{scene_id}",
                    player["player_id"],
                    bytes(source_blob["data"]),
                    str(source_blob["image_list_json"] or "{}"),
                ),
            )
        row = conn.execute("SELECT * FROM rooms WHERE room_id = ?", (canonical_id,)).fetchone()
    record = _ugc_record_from_row(row)
    return JSONResponse({
        "Result": 0,
        "RoomDetails": _serialize_ugc_details(
            record, local_player_id=_legacy_id_for_player(player)
        ),
    })


async def _handle_modify_room(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    record = _find_ugc_room(context, room_id=room_id)
    if record is None:
        return JSONResponse({"Result": 4, "RoomDetails": None})
    if not _player_can_edit_ugc(record, player):
        return JSONResponse({"Result": 2, "RoomDetails": None})
    metadata = record["metadata"]
    name = _BASE._str_field(payload, "Name", "name", default=str(record["row"]["name"])).strip()
    description = _BASE._str_field(payload, "Description", "description", default=str(metadata.get("description") or ""))
    if not name or len(name) > 32:
        return JSONResponse({"Result": 12, "RoomDetails": None})
    if len(description) > 512:
        return JSONResponse({"Result": 1, "RoomDetails": None})
    name = _filter_user_text(
        context,
        name,
        policy="censor",
        field_context="room.name",
        player=player,
    )
    description = _filter_user_text(
        context,
        description,
        policy="censor",
        field_context="room.description",
        player=player,
    )
    accessibility = _BASE._int_field(payload, "Accessibility", "accessibility", default=int(metadata.get("accessibility", 0)))
    if accessibility not in {0, 1, 2}:
        return JSONResponse({"Result": 1, "RoomDetails": None})
    if bool(metadata.get("moderation_restricted", False)) and accessibility != 0:
        return JSONResponse({"Result": 2, "RoomDetails": None})
    image_field_present = "ImageName" in payload or "imageName" in payload
    image_name = _BASE._str_field(
        payload, "ImageName", "imageName", default=str(metadata.get("image_name") or "")
    ).strip()
    if image_field_present:
        # UploadSaved always creates a fresh UUID filename. Only let a room
        # reference an image owned by the same authenticated player.
        if Path(image_name).name != image_name or not re.fullmatch(
            r"[0-9a-f-]{36}\.(?:jpg|jpeg|png)", image_name, flags=re.IGNORECASE
        ):
            return JSONResponse({"Result": 1, "RoomDetails": None})
        with context.db.connection() as conn:
            image_asset = conn.execute(
                """
                SELECT asset_id FROM data_assets
                WHERE (relative_path = ? OR relative_path = ? OR relative_path LIKE ?)
                  AND owner_player_id = ?
                  AND purpose = ?
                LIMIT 1
                """,
                (
                    f"IMAGES/{image_name}",
                    image_name,
                    f"%/{image_name}",
                    player["player_id"],
                    f"{API_VERSION}.saved_image",
                ),
            ).fetchone()
        if image_asset is None or not context.image_asset_is_available(
            str(image_asset["asset_id"])
        ):
            return JSONResponse({"Result": 2, "RoomDetails": None})
        metadata["image_name"] = image_name
    metadata.update({
        "description": description[:512],
        "accessibility": accessibility,
        "cloning_allowed": _bool_value(
            payload.get("ShouldAllowCloning", metadata.get("cloning_allowed", True))
        ),
        "supports_screens": _bool_value(
            payload.get("SupportsScreens", metadata.get("supports_screens", True))
        ),
        "supports_walk_vr": _bool_value(
            payload.get("SupportsWalkVR", metadata.get("supports_walk_vr", True))
        ),
        "supports_teleport_vr": _bool_value(
            payload.get("SupportsTeleportVR", metadata.get("supports_teleport_vr", True))
        ),
        "allows_juniors": _bool_value(
            payload.get("AllowsJuniors", metadata.get("allows_juniors", True))
        ),
        "disable_mic_auto_mute": _bool_value(
            payload.get("DisableMicAutoMute", metadata.get("disable_mic_auto_mute", False))
        ),
    })
    # Automatic tags are recomputed when the owner changes the searchable
    # name/description. They are intentionally not sourced from the request.
    record_for_tags = {**record, "metadata": metadata}
    record_for_tags["row"] = {**record["row"], "name": name}
    metadata["auto_tags"] = _infer_room_auto_tags(record_for_tags)
    with context.db.transaction() as conn:
        duplicate = conn.execute(
            "SELECT 1 FROM rooms WHERE lower(name) = lower(?) AND room_id <> ?", (name, record["row"]["room_id"])
        ).fetchone()
        if duplicate is not None:
            return JSONResponse({"Result": 10, "RoomDetails": None})
        conn.execute(
            "UPDATE rooms SET name = ?, metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE room_id = ?",
            (name, json.dumps(metadata, sort_keys=True), record["row"]["room_id"]),
        )
        row = conn.execute("SELECT * FROM rooms WHERE room_id = ?", (record["row"]["room_id"],)).fetchone()
    if image_field_present:
        _update_event_images_for_room(context, room_id, image_name)
    return JSONResponse({
        "Result": 0,
        "RoomDetails": _serialize_ugc_details(
            _ugc_record_from_row(row),
            local_player_id=_legacy_id_for_player(player),
        ),
    })


async def _handle_modify_room_permissions(request: Request, context) -> Response:
    """Persist RoomPermissionValue fields while preserving omitted values."""
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    target_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    record = _find_ugc_room(context, room_id=room_id)
    if record is None:
        return JSONResponse({"Result": 4, "RoomDetails": None})
    creator_id = int(record["creator_player_id"])
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target_id <= 0 or target_id == creator_id or target is None:
        return JSONResponse({"Result": 1, "RoomDetails": None})

    supplied: dict[str, int] = {}
    for wire_name, storage_name in (("IsOwner", "co_owner"), ("IsHost", "host")):
        matching_key = next(
            (key for key in payload if str(key).casefold() == wire_name.casefold()), None
        )
        if matching_key is None:
            continue
        raw_value = payload[matching_key]
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            return JSONResponse({"Result": 1, "RoomDetails": None})
        if value not in {0, 1, 2}:
            return JSONResponse({"Result": 1, "RoomDetails": None})
        supplied[storage_name] = value
    if not supplied:
        return JSONResponse({"Result": 1, "RoomDetails": None})

    metadata = record["metadata"]
    previous_values: dict[str, int] = {}
    for role_name in supplied:
        active = {
            int(item) for item in metadata.get(f"{role_name}_ids", [])
            if str(item).lstrip("-").isdigit() and int(item) > 0
        }
        invited = {
            int(item) for item in metadata.get(f"invited_{role_name}_ids", [])
            if str(item).lstrip("-").isdigit() and int(item) > 0
        }
        previous_values[role_name] = (
            1 if target_id in active else (2 if target_id in invited else 0)
        )

    actor_is_owner = record["row"]["owner_player_id"] == player["player_id"]
    local_id = _legacy_id_for_player(player)
    invited_player_response = (
        target_id == local_id
        and supplied.keys() == {"co_owner"}
        and supplied["co_owner"] in {0, 1}
        and previous_values.get("co_owner") == 2
    )
        # Invitees may accept or reject only their own co-owner invitation.
    if not actor_is_owner and not invited_player_response:
        return JSONResponse({"Result": 2, "RoomDetails": None})

    for role_name, value in supplied.items():
        active_key = f"{role_name}_ids"
        invited_key = f"invited_{role_name}_ids"
        active = {
            int(item) for item in metadata.get(active_key, [])
            if str(item).lstrip("-").isdigit() and int(item) > 0
        }
        invited = {
            int(item) for item in metadata.get(invited_key, [])
            if str(item).lstrip("-").isdigit() and int(item) > 0
        }
        active.discard(target_id)
        invited.discard(target_id)
        if value == 1:
            active.add(target_id)
        elif value == 2:
            invited.add(target_id)
        metadata[active_key] = sorted(active)
        metadata[invited_key] = sorted(invited)

    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE rooms
            SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE room_id = ?
            """,
            (json.dumps(metadata, sort_keys=True), record["row"]["room_id"]),
        )
        row = conn.execute(
            "SELECT * FROM rooms WHERE room_id = ?", (record["row"]["room_id"],)
        ).fetchone()

    if "co_owner" in supplied:
        new_value = supplied["co_owner"]
        old_value = previous_values.get("co_owner", 0)
        if actor_is_owner and new_value != old_value:
            message_type = (
                62 if new_value == 2
                else (60 if new_value == 1 else (61 if old_value == 1 else None))
            )
            if message_type is not None:
                await _create_recnet_message(
                    target,
                    from_player_id=creator_id,
                    message_type=message_type,
                    room_id=room_id,
                    context=context,
                )
        elif invited_player_response:
            # Remove the durable invitation after the dialog consumes it.
            _remove_recnet_messages(
                target,
                context,
                from_player_id=creator_id,
                message_types={62},
            )
    return JSONResponse(
        {
            "Result": 0,
            "RoomDetails": _serialize_ugc_details(
                _ugc_record_from_row(row),
                local_player_id=_legacy_id_for_player(player),
            ),
        }
    )


async def _handle_report_room(request: Request, context) -> Response:
    reporter = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    category = _BASE._int_field(payload, "ReportCategory", "reportCategory", default=0)
    details = _BASE._str_field(payload, "Details", "details", default="").strip()[:2000]
    room_exists = bool(
        _find_coach_room_by_id(room_id, context) is not None
        or _find_ugc_room(context, room_id=room_id) is not None
        or _find_dorm_room_by_room_id(context, room_id) is not None
    )
    valid_categories = {-1, 0, 1, 2, 3, 4, 5, 6, 7, 10, 100, 101, 102, 103, 104, 1000}
    if not room_exists or category not in valid_categories:
        return JSONResponse({"Success": False, "Message": "Invalid room report."})
    _submit_canonical_report(
        reporter=reporter,
        target_type="room",
        target_id=room_id,
        raw_category=category,
        canonical_category=PLAYER_REPORT_CATEGORY_MAP.get(category, "unknown"),
        category_schema="room_reporting_v2",
        details=details,
        room_id=room_id,
        game_session_id=None,
        source_endpoint="api/rooms/v2/report",
        source_payload=payload,
        context=context,
    )
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_modify_scene_parent(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    scene_id = _BASE._int_field(payload, "RoomSceneId", "roomSceneId", default=0)
    parent_room_id = _BASE._int_field(
        payload, "NewParentRoomId", "newParentRoomId", default=0
    )
    record = _find_ugc_room_by_scene_id(context, scene_id)
    if record is None:
        return JSONResponse({"Result": 4, "RoomDetails": None})
    if not _player_can_edit_ugc(record, player):
        return JSONResponse({"Result": 2, "RoomDetails": None})
    parent_exists = (
        _find_coach_room_by_id(parent_room_id, context) is not None
        or _find_ugc_room(context, room_id=parent_room_id) is not None
        or _find_dorm_room_by_room_id(context, parent_room_id) is not None
    )
    if not parent_exists:
        return JSONResponse({"Result": 4, "RoomDetails": None})
    record["metadata"]["parent_room_id"] = parent_room_id
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE rooms
            SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE room_id = ?
            """,
            (json.dumps(record["metadata"], sort_keys=True), record["row"]["room_id"]),
        )
        row = conn.execute(
            "SELECT * FROM rooms WHERE room_id = ?", (record["row"]["room_id"],)
        ).fetchone()
    updated = _ugc_record_from_row(row)
    return JSONResponse({
        "Result": 0,
        "RoomDetails": _serialize_ugc_details(
            updated, local_player_id=_legacy_id_for_player(player)
        ),
    })


async def _handle_modify_room_scene(request: Request, context) -> Response:
    """Apply the exact v1/modifyscene request to a player-owned UGC scene."""
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    scene_id = _BASE._int_field(payload, "RoomSceneId", "roomSceneId", default=0)
    record = _find_ugc_room_by_scene_id(context, scene_id)
    if record is None:
        return JSONResponse({"Result": 4, "RoomScene": None})
    if not _player_can_edit_ugc(record, player):
        return JSONResponse({"Result": 2, "RoomScene": None})

    name = _BASE._str_field(
        payload,
        "Name",
        "name",
        default=str(record["version"].get("room_scene_name") or "Home"),
    ).strip()
    max_players = _BASE._int_field(
        payload,
        "MaxPlayers",
        "maxPlayers",
        default=int(record["version"].get("max_players") or 20),
    )
    raw_matchmaking = payload.get(
        "CanMatchmakeInto",
        payload.get("canMatchmakeInto", record["version"].get("can_matchmake_into", True)),
    )
    can_matchmake = (
        raw_matchmaking.strip().casefold() in {"true", "1", "yes", "on"}
        if isinstance(raw_matchmaking, str)
        else bool(raw_matchmaking)
    )
    if not name or len(name) > 32 or max_players < 1 or max_players > 40:
        return JSONResponse({"Result": 1, "RoomScene": None})
    name = _filter_user_text(
        context,
        name,
        policy="censor",
        field_context="room.scene_name",
        player=player,
    )

    record["version"]["room_scene_name"] = name
    record["version"]["max_players"] = max_players
    record["version"]["can_matchmake_into"] = can_matchmake
    with context.db.transaction() as conn:
        instances = _read_game_instances(conn)
        instances_changed = False
        for instance in instances:
            if int(instance.get("RoomSceneId") or 0) != scene_id:
                continue
            instance["MaxCapacity"] = max_players
            instance["can_matchmake_into"] = can_matchmake
            instances_changed = True
        if instances_changed:
            _write_game_instances(conn, instances)
        conn.execute(
            """
            UPDATE rooms
            SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE room_id = ?
            """,
            (json.dumps(record["metadata"], sort_keys=True), record["row"]["room_id"]),
        )
        row = conn.execute(
            "SELECT * FROM rooms WHERE room_id = ?", (record["row"]["room_id"],)
        ).fetchone()
    refreshed = _ugc_record_from_row(row)
    return JSONResponse({"Result": 0, "RoomScene": _serialize_ugc_scene(refreshed)})


async def _handle_modify_room_tags(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    record = _find_ugc_room(context, room_id=room_id)
    if record is None:
        return JSONResponse({"Result": 11, "Tags": []})
    if not _player_can_edit_ugc(record, player):
        return JSONResponse({"Result": 10, "Tags": []})

    auto_tags: list[str] = []
    custom_tags: list[str] = []

    def collect(keys: tuple[str, ...], destination: list[str]) -> None:
        for key in keys:
            values = payload.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                tag = str(value).strip().lstrip("#")
                if not tag or tag.casefold() in {
                    item.casefold() for item in [*auto_tags, *custom_tags]
                }:
                    continue
                if len(tag) > 15 or not tag.replace(" ", "").isalnum():
                    raise ValueError(tag)
                tag = _filter_user_text(
                    context,
                    tag,
                    policy="censor",
                    field_context="room.tag",
                    player=player,
                )
                if tag.casefold() in {
                    item.casefold() for item in [*auto_tags, *custom_tags]
                }:
                    continue
                destination.append(tag)

    try:
        collect(("AutoTags", "autoTags"), auto_tags)
        collect(("CustomTags", "customTags", "Tags", "tags"), custom_tags)
    except ValueError:
        return JSONResponse({"Result": 3, "Tags": [*auto_tags, *custom_tags]})
    if len(custom_tags) > 3 or len(auto_tags) + len(custom_tags) > 10:
        return JSONResponse({"Result": 1, "Tags": [*auto_tags, *custom_tags][:10]})

    metadata = record["metadata"]
    metadata["custom_tags"] = custom_tags
    # Infer auto-tags only when the save omits object-derived tags.
    metadata["auto_tags"] = auto_tags or _infer_room_auto_tags(record)
    metadata["tags"] = [
        f"#{tag}" for tag in dict.fromkeys(
            [*metadata["auto_tags"], *metadata["custom_tags"]]
        )
    ]
    with context.db.transaction() as conn:
        conn.execute(
            "UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE room_id = ?",
            (json.dumps(metadata, sort_keys=True), record["row"]["room_id"]),
        )
    return JSONResponse({
        "Result": 0,
        "Tags": [*metadata["auto_tags"], *metadata["custom_tags"]],
    })


def _legacy_id_for_player(player) -> int:
    state = _player_state(player)
    return int(state.get("legacy_player_id") or state.get("recnet_id") or 0)


def _read_game_instances(conn) -> list[dict[str, Any]]:
    row = conn.execute(
        "SELECT value_json FROM server_settings WHERE key = ?", (GAME_INSTANCES_SETTING,)
    ).fetchone()
    if row is None:
        return []
    try:
        instances = json.loads(row["value_json"] or "[]")
    except Exception:
        return []
    if not isinstance(instances, list):
        return []
    cleaned: list[dict[str, Any]] = []
    for item in instances:
        if not isinstance(item, dict):
            continue
        item = dict(item)
        for transient_key in (
            "members",
            "vote_to_kick",
            "invited",
            "invite_grants",
            "party_members",
        ):
            item.pop(transient_key, None)
        cleaned.append(item)
    return cleaned


def _write_game_instances(conn, instances: list[dict[str, Any]]) -> None:
    durable_instances: list[dict[str, Any]] = []
    for instance in instances:
        durable = dict(instance)
        for transient_key in (
            "members",
            "vote_to_kick",
            "invited",
            "invite_grants",
            "party_members",
        ):
            durable.pop(transient_key, None)
        durable_instances.append(durable)
    conn.execute(
        """
        INSERT INTO server_settings(key, value_json, created_at, updated_at)
        VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
            updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        """,
        (GAME_INSTANCES_SETTING, json.dumps(durable_instances, sort_keys=True)),
    )


def _public_game_session(
    instance: dict[str, Any], *, member_count: int | None = None
) -> dict[str, Any]:
    if member_count is None:
        member_count = 0
    max_capacity = int(instance["MaxCapacity"])
    return {
        key: instance.get(key)
        for key in (
            "GameSessionId", "PhotonRegionId", "PhotonRoomId", "Name", "RoomId",
            "RoomSceneId", "RoomSceneLocationId", "IsSandbox", "DataBlobName",
            "PlayerEventId", "Private", "GameInProgress", "MaxCapacity"
        )
    } | {"IsFull": member_count >= max_capacity}


def _instance_player_ids(instance: dict[str, Any], key: str) -> set[int]:
    values = instance.get(key)
    if not isinstance(values, list):
        return set()
    return {
        int(value)
        for value in values
        if str(value).lstrip("-").isdigit() and int(value) > 0
    }


async def _valid_instance_invite(
    instance: dict[str, Any], player_id: int, context
) -> bool:
    """An invite remains valid only while its sender is still in that instance."""
    members = {
        int(value)
        for value in await context.require_transient().session_member_ids(
            int(instance.get("GameSessionId") or 0)
        )
        if str(value).lstrip("-").isdigit()
    }
    inviter = await context.require_transient().session_inviter_id(
        int(instance.get("GameSessionId") or 0), player_id
    )
    if not inviter or not inviter.lstrip("-").isdigit():
        return False
    return int(inviter) in members


async def _instance_inviter_ids(
    instance: dict[str, Any], player_id: int, context
) -> set[int]:
    members = {
        int(value)
        for value in await context.require_transient().session_member_ids(
            int(instance.get("GameSessionId") or 0)
        )
        if str(value).lstrip("-").isdigit()
    }
    inviter = await context.require_transient().session_inviter_id(
        int(instance.get("GameSessionId") or 0), player_id
    )
    if not inviter or not inviter.lstrip("-").isdigit():
        return set()
    inviter_id = int(inviter)
    return {inviter_id} if inviter_id in members else set()


async def _record_instance_invites(
    instance: dict[str, Any], inviter_id: int, invited_player_ids: list[int], context
) -> None:
    invited_ids = {
        int(value) for value in invited_player_ids
        if str(value).lstrip("-").isdigit()
        and int(value) > 0
        and int(value) != inviter_id
    }
    await context.require_transient().record_session_invites(
        int(instance.get("GameSessionId") or 0), inviter_id, sorted(invited_ids)
    )


async def _consume_instance_invite(
    instance: dict[str, Any], player_id: int, context
) -> None:
    await context.require_transient().consume_session_invite(
        int(instance.get("GameSessionId") or 0), player_id
    )


async def _authorize_party_travel(
    instance: dict[str, Any], local_id: int, expected_player_ids: list[int] | None, context
) -> None:
    party_members = {
        int(value)
        for value in (expected_player_ids or [])
        if str(value).lstrip("-").isdigit() and int(value) > 0
    }
    await context.require_transient().authorize_session_party(
        int(instance.get("GameSessionId") or 0), local_id, sorted(party_members)
    )


def _expected_player_ids(payload: dict[str, Any], local_id: int) -> list[int]:
    raw_values: Any = []
    for key in (
        "ExpectedPlayerIds",
        "ExpectedPlayerIDs",
        "expectedPlayerIds",
        "expectedPlayerIDs",
    ):
        if key in payload:
            raw_values = payload[key]
            break
    if raw_values == []:
        indexed_values = [
            value
            for key, value in payload.items()
            if re.fullmatch(
                r"expectedplayerids?(?:\[\d+\]|\.\d+)",
                str(key),
                flags=re.IGNORECASE,
            )
        ]
        if indexed_values:
            raw_values = indexed_values

    # Accept every BaseJoinRequest list representation used by this client.
    if isinstance(raw_values, dict):
        raw_values = raw_values.get("$values", raw_values.get("Values", []))
    if isinstance(raw_values, str):
        text = raw_values.strip()
        if not text:
            raw_values = []
        else:
            try:
                decoded = json.loads(text)
            except (TypeError, ValueError, json.JSONDecodeError):
                decoded = re.split(r"[\s,;|]+", text.strip("[](){}"))
            raw_values = decoded if isinstance(decoded, (list, tuple, set)) else [decoded]
    elif not isinstance(raw_values, (list, tuple, set)):
        raw_values = [] if raw_values in (None, "") else [raw_values]

    return sorted({
        int(value)
        for value in raw_values
        if str(value).lstrip("-").isdigit()
        and int(value) > 0
        and int(value) != local_id
    })


def _party_auto_follow_requested(payload: dict[str, Any]) -> bool:
    raw_value = payload.get(
        "AdditionalPlayerJoinMode",
        payload.get("additionalPlayerJoinMode", 0),
    )
    if isinstance(raw_value, str) and raw_value.casefold() == "autofollow":
        return True
    try:
        return int(raw_value) == 1
    except (TypeError, ValueError):
        return False


async def _notify_party_activity_switch(
    source_id: int,
    expected_player_ids: list[int],
    game_session: dict[str, Any],
    context,
) -> None:
    """Deliver the 2019 PartyActivitySwitch message after AutoFollow travel."""
    room_id = int(game_session.get("RoomId") or 0)
    for target_id in expected_player_ids:
        target = _find_player_by_legacy_id_25april2019(context, target_id)
        if target is None or _players_ignore_each_other(context, source_id, target_id):
            continue
        # Only the newest destination from this party member is actionable.
        _remove_recnet_messages(
            target,
            context,
            from_player_id=source_id,
            message_types={3},
        )
        await _create_recnet_message(
            target,
            from_player_id=source_id,
            message_type=3,
            room_id=room_id,
            context=context,
        )


async def _notify_join_companions(
    source_id: int,
    expected_player_ids: list[int],
    game_session: dict[str, Any],
    context,
    *,
    auto_follow: bool,
) -> None:
    if not expected_player_ids:
        return
    if auto_follow:
        await _notify_party_activity_switch(
            source_id, expected_player_ids, game_session, context
        )
        return

    targets: list[tuple[int, Any]] = []
    for target_id in expected_player_ids:
        target = _find_player_by_legacy_id_25april2019(context, target_id)
        if target is None or _players_ignore_each_other(context, source_id, target_id):
            continue
        targets.append((target_id, target))
    if not targets:
        return

    game_session_id = int(game_session.get("GameSessionId") or 0)
    room_id = int(game_session.get("RoomId") or 0)
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        instance = next(
            (
                item for item in instances
                if int(item.get("GameSessionId") or 0) == game_session_id
            ),
            None,
        )
        if instance is None:
            return
        await _record_instance_invites(
            instance, source_id, [target_id for target_id, _target in targets], context
        )
        _write_game_instances(conn, instances)

    for _target_id, target in targets:
        # Keep only the sender's current destination invite.
        _remove_recnet_messages(
            target,
            context,
            from_player_id=source_id,
            message_types={0},
        )
        await _create_recnet_message(
            target,
            from_player_id=source_id,
            message_type=0,
            room_id=room_id,
            context=context,
        )


async def _can_enter_private_instance(
    instance: dict[str, Any], player_id: int, context
) -> bool:
    members = {
        int(value)
        for value in await context.require_transient().session_member_ids(
            int(instance.get("GameSessionId") or 0)
        )
        if str(value).lstrip("-").isdigit()
    }
    return (
        player_id in members
        or str(player_id)
        in await context.require_transient().session_party_member_ids(
            int(instance.get("GameSessionId") or 0)
        )
        or await _valid_instance_invite(instance, player_id, context)
    )


async def _repair_player_game_session(
    player, requested_session_id: int, context
) -> dict[str, Any] | None:
    """Repair a Redis membership only when durable identity/invite data proves it."""
    if requested_session_id <= 0:
        return None
    local_id = _legacy_id_for_player(player)
    state = _player_state(player)
    current = await context.require_transient().get_membership(local_id)
    if (
        isinstance(current, dict)
        and int(current.get("GameSessionId") or 0) == requested_session_id
    ):
        return current
    stored = state.get("game_session")
    stored_id = int(stored.get("GameSessionId") or 0) if isinstance(stored, dict) else 0
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        target = next(
            (
                item for item in instances
                if int(item.get("GameSessionId") or 0) == requested_session_id
            ),
            None,
        )
        if target is None:
            return None
        authorized = (
            stored_id == requested_session_id
            or str(local_id)
            in await context.require_transient().session_party_member_ids(
                requested_session_id
            )
            or await _valid_instance_invite(target, local_id, context)
        )
        if not authorized:
            return None

        had_invite = await _valid_instance_invite(target, local_id, context)
        await _consume_instance_invite(target, local_id, context)
        if had_invite:
            _write_game_instances(conn, instances)
        active = _public_game_session(target)

    await context.require_transient().set_membership(local_id, active)
    _schedule_presence_update(local_id, context)
    return active


async def _active_game_session_for_player(player, context) -> dict[str, Any] | None:
    local_id = _legacy_id_for_player(player)
    state = _player_state(player)
    active = await _authoritative_game_session_for_player(local_id, state, context)
    if active is None:
        stored = state.get("game_session")
        stored_id = int(stored.get("GameSessionId") or 0) if isinstance(stored, dict) else 0
        active = await _repair_player_game_session(player, stored_id, context)
    return active


async def _join_coach_instance(
    player, record: dict[str, Any], scene: dict[str, Any], context, *, private: bool,
    invited_player_ids: list[int] | None = None,
) -> dict[str, Any]:
    local_id = _legacy_id_for_player(player)
    scene_id = BUILD_COACH_SCENE_IDS[(str(record["asset"]["n"]).casefold(), str(scene["n"]).casefold())]
    previous_session = await context.require_transient().get_membership(local_id)
    previous_session_id = (
        int(previous_session.get("GameSessionId") or 0)
        if isinstance(previous_session, dict)
        else 0
    )
    previous_scene_id = (
        int(previous_session.get("RoomSceneId") or 0)
        if isinstance(previous_session, dict)
        else 0
    )
    # Exclude the current scene instance when matchmaking a fresh JoinRoom.
    excluded_session_id = previous_session_id if previous_scene_id == scene_id else 0
    selected_member_count = 0
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        match = None
        if not private and bool(scene.get("q", False)):
            arriving_ids = {
                local_id,
                *(
                    int(value) for value in (invited_player_ids or [])
                    if str(value).lstrip("-").isdigit() and int(value) > 0
                ),
            }
            for item in instances:
                members = {
                    int(value)
                    for value in await context.require_transient().session_member_ids(
                        int(item.get("GameSessionId") or 0)
                    )
                    if str(value).lstrip("-").isdigit()
                }
                if (
                    int(item.get("RoomSceneId") or 0) == scene_id
                    and int(item.get("GameSessionId") or 0) != excluded_session_id
                    and not bool(item.get("Private", False))
                    and len(members | arriving_ids) <= int(item.get("MaxCapacity") or 0)
                    and (not bool(item.get("GameInProgress", False)) or bool(scene.get("p", False)))
                ):
                    match = item
                    selected_member_count = len(members)
                    break

        created_instance = match is None
        if match is None:
            game_session_id = max(
                (int(item.get("GameSessionId") or 0) for item in instances), default=99_999
            ) + 1
            record_with_context = {**record, "context": context}
            serialized_scene = _serialize_coach_scene(record_with_context, scene)
            match = {
                "GameSessionId": game_session_id,
                "PhotonRegionId": "us",
                "PhotonRoomId": f"rr-{API_VERSION}-{game_session_id}",
                "Name": str(record["asset"]["n"]),
                "RoomId": int(record["room_id"]),
                "RoomSceneId": scene_id,
                "RoomSceneLocationId": str(scene["l"]),
                "IsSandbox": bool(scene.get("b", False)),
                "DataBlobName": serialized_scene["DataBlobName"],
                "PlayerEventId": None,
                "Private": bool(private),
                "GameInProgress": False,
                "MaxCapacity": _coach_scene_max_players(record, scene),
                "supports_join_in_progress": bool(scene.get("p", False)),
                "is_coach_room": True,
            }
            instances.append(match)
            selected_member_count = 0
        await _authorize_party_travel(match, local_id, invited_player_ids, context)
        new_visit = local_id not in {
            int(value)
            for value in await context.require_transient().session_member_ids(
                int(match.get("GameSessionId") or 0)
            )
            if str(value).lstrip("-").isdigit()
        }
        await _consume_instance_invite(match, local_id, context)
        _write_game_instances(conn, instances)
        metric_row = conn.execute(
            "SELECT metadata_json FROM rooms WHERE room_id = ?", (record["canonical_room_id"],)
        ).fetchone()
        try:
            metrics = json.loads(metric_row["metadata_json"] or "{}") if metric_row else {}
        except Exception:
            metrics = {}
        if isinstance(metrics, dict):
            if new_visit:
                metrics["visit_count"] = int(metrics.get("visit_count", 0) or 0) + 1
            if created_instance:
                metrics["game_session_count"] = int(metrics.get("game_session_count", 0) or 0) + 1
            conn.execute(
                "UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE room_id = ?",
                (json.dumps(metrics, sort_keys=True), record["canonical_room_id"]),
            )
    return _public_game_session(
        match, member_count=selected_member_count + (1 if new_visit else 0)
    )


async def _register_dorm_instance(
    player, dorm: dict[str, Any], context, invited_player_ids: list[int] | None = None
) -> dict[str, Any]:
    local_id = _legacy_id_for_player(player)
    locally_banned = {
        int(value) for value in dorm["metadata"].get("banned_player_ids", [])
        if str(value).lstrip("-").isdigit()
    }
    if local_id in locally_banned:
        raise HTTPException(status_code=403, detail="You are banned from this room.")
    game_session = _serialize_dorm_game_session(dorm)
    member_count = 0
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        instance = next(
            (item for item in instances if int(item.get("GameSessionId") or 0) == int(game_session["GameSessionId"])),
            None,
        )
        if instance is not None and (
            not bool(instance.get("is_dorm", False))
            or int(instance.get("owner_player_id") or 0) != local_id
        ):
            new_session_id = max(
                (int(item.get("GameSessionId") or 0) for item in instances), default=99_999
            ) + 1
            dorm["version"]["game_session_id"] = new_session_id
            dorm["version"]["photon_room_id"] = f"dorm-{new_session_id}"
            conn.execute(
                "UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE room_id = ?",
                (json.dumps(dorm["metadata"], sort_keys=True), dorm["row"]["room_id"]),
            )
            game_session = _serialize_dorm_game_session(dorm)
            instance = None
        if instance is None:
            instance = {
                **game_session,
                "supports_join_in_progress": True,
                "is_coach_room": False,
                "is_dorm": True,
                "owner_player_id": local_id,
                "banned": sorted(locally_banned),
            }
            instances.append(instance)
        members = await context.require_transient().session_member_ids(
            int(instance.get("GameSessionId") or 0)
        )
        member_count = len(members) + (0 if str(local_id) in members else 1)
        await _authorize_party_travel(instance, local_id, invited_player_ids, context)
        await _consume_instance_invite(instance, local_id, context)
        _write_game_instances(conn, instances)
    return _public_game_session(instance, member_count=member_count)


async def _join_ugc_instance(
    player, record: dict[str, Any], context, *, private: bool,
    invited_player_ids: list[int] | None = None,
) -> dict[str, Any]:
    local_id = _legacy_id_for_player(player)
    locally_banned = {
        int(value) for value in record["metadata"].get("banned_player_ids", [])
        if str(value).lstrip("-").isdigit()
    }
    if local_id in locally_banned:
        raise HTTPException(status_code=403, detail="You are banned from this room.")
    scene = _serialize_ugc_scene(record)
    previous_session = await context.require_transient().get_membership(local_id)
    previous_session_id = (
        int(previous_session.get("GameSessionId") or 0)
        if isinstance(previous_session, dict)
        else 0
    )
    previous_scene_id = (
        int(previous_session.get("RoomSceneId") or 0)
        if isinstance(previous_session, dict)
        else 0
    )
    scene_id = int(scene["RoomSceneId"])
    excluded_session_id = previous_session_id if previous_scene_id == scene_id else 0
    selected_member_ids: set[int] = set()
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        match = None
        if not private:
            arriving_ids = {
                local_id,
                *(
                    int(value) for value in (invited_player_ids or [])
                    if str(value).lstrip("-").isdigit() and int(value) > 0
                ),
            }
            for item in instances:
                member_ids = {
                    int(value)
                    for value in await context.require_transient().session_member_ids(
                        int(item.get("GameSessionId") or 0)
                    )
                    if str(value).lstrip("-").isdigit()
                }
                if (
                    int(item.get("RoomSceneId") or 0) == scene_id
                    and int(item.get("GameSessionId") or 0) != excluded_session_id
                    and not bool(item.get("Private", False))
                    and len(member_ids | arriving_ids)
                    <= int(item.get("MaxCapacity") or 0)
                ):
                    match = item
                    selected_member_ids = member_ids
                    break
        if match is None:
            game_session_id = max(
                (int(item.get("GameSessionId") or 0) for item in instances),
                default=int(record["version"].get("game_session_id") or 99_999) - 1,
            ) + 1
            match = {
                "GameSessionId": game_session_id,
                "PhotonRegionId": "us",
                "PhotonRoomId": f"rr-{API_VERSION}-ugc-{game_session_id}",
                "Name": str(record["row"]["name"]),
                "RoomId": int(scene["RoomId"]),
                "RoomSceneId": int(scene["RoomSceneId"]),
                "RoomSceneLocationId": str(scene["RoomSceneLocationId"]),
                "IsSandbox": True,
                "DataBlobName": str(scene["DataBlobName"]),
                "PlayerEventId": None,
                "Private": bool(private),
                "GameInProgress": False,
                "MaxCapacity": int(scene["MaxPlayers"]),
                "supports_join_in_progress": True,
                "is_coach_room": False,
                "is_ugc": True,
                "banned": sorted(locally_banned),
            }
            instances.append(match)
            selected_member_ids = set()
            record["metadata"]["game_session_count"] = int(record["metadata"].get("game_session_count", 0) or 0) + 1
        await _authorize_party_travel(match, local_id, invited_player_ids, context)
        if local_id not in selected_member_ids:
            record["metadata"]["visit_count"] = int(record["metadata"].get("visit_count", 0) or 0) + 1
        await _consume_instance_invite(match, local_id, context)
        _write_game_instances(conn, instances)
        conn.execute(
            "UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE room_id = ?",
            (json.dumps(record["metadata"], sort_keys=True), record["row"]["room_id"]),
        )
    return _public_game_session(
        match,
        member_count=len(selected_member_ids) + (0 if local_id in selected_member_ids else 1),
    )


def _find_scene(record: dict[str, Any], scene_name: str) -> dict[str, Any] | None:
    scenes = record["asset"]["x"]
    if not scene_name:
        return scenes[0] if scenes else None
    return next((scene for scene in scenes if str(scene["n"]).casefold() == scene_name.casefold()), None)


async def _persist_active_game_session(player, game_session: dict[str, Any], context) -> None:
    await context.require_transient().set_membership(
        _legacy_id_for_player(player), game_session
    )
    room_id = int(game_session.get("RoomId") or 0)
    if room_id > 0:
        key = _canonical_player_setting_key("recent_rooms", player["player_id"])
        recent = _BASE._get_json_setting(context, key, [])
        if not isinstance(recent, list):
            recent = []
        canonical = [
            int(value) for value in recent
            if str(value).lstrip("-").isdigit() and int(value) > 0 and int(value) != room_id
        ]
        _BASE._set_json_setting(context, key, [room_id, *canonical][:20])
    _schedule_presence_update(_legacy_id_for_player(player), context)


async def _clear_active_game_session_for_login(player, state: dict[str, Any], context) -> None:
    legacy_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
    state.pop("game_session", None)
    state.pop("last_presence_heartbeat_at", None)
    if legacy_id <= 0:
        return
    await context.require_transient().set_membership(legacy_id, None)


def _persist_player_state(player, state: dict[str, Any], context) -> None:
    state = dict(state)
    for transient_key in (
        "game_session",
        "last_presence_heartbeat_at",
        "login_token",
        "presence_login_lock_token",
    ):
        state.pop(transient_key, None)
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )


def _patch_player_state(
    player,
    context,
    *,
    set_values: dict[str, Any] | None = None,
    remove_keys: set[str] | None = None,
) -> dict[str, Any]:
    """Atomically update selected state fields without replacing unrelated values."""
    with context.db.transaction() as conn:
        row = conn.execute(
            """
            SELECT state_json
            FROM player_version_state
            WHERE player_id = ? AND api_version = ?
            """,
            (player["player_id"], API_VERSION),
        ).fetchone()
        try:
            state = json.loads(row["state_json"] or "{}") if row is not None else {}
        except Exception:
            state = {}
        if not isinstance(state, dict):
            state = {}
        for transient_key in (
            "game_session",
            "last_presence_heartbeat_at",
            "login_token",
            "presence_login_lock_token",
        ):
            state.pop(transient_key, None)
        for key in remove_keys or set():
            state.pop(key, None)
        state.update(set_values or {})
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )
    if isinstance(player, dict):
        player["state"] = state
    return state


async def _parse_json_list(request: Request, *, payload_name: str) -> list[dict[str, Any]]:
    body = await request.body()
    if not body:
        return []
    try:
        payload = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {payload_name} payload.") from exc
    if isinstance(payload, dict):
        payload = payload.get(payload_name) or payload.get(payload_name.casefold()) or []
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail=f"{payload_name} payload must be a list.")
    if any(not isinstance(item, dict) for item in payload):
        raise HTTPException(status_code=400, detail=f"Every {payload_name} entry must be an object.")
    return payload


async def _handle_report_game_join_result(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    game_session_id = _BASE._int_field(payload, "GameSessionId", "gameSessionId", default=0)
    result = _BASE._int_field(payload, "Result", "result", default=-1)
    if game_session_id <= 0 or result not in {0, 1, 2, 3}:
        raise HTTPException(status_code=400, detail="GameSessionId and a valid Result are required.")

    game_session = await _repair_player_game_session(player, game_session_id, context)
    if game_session is None and result == 0:
        raise HTTPException(status_code=409, detail="The reported game session is not the player's active session.")

    # ReportGameJoinResultRequest in this build has exactly these six fields.
    last_game_join_result = {
        "GameSessionId": game_session_id,
        "RegionId": _BASE._str_field(payload, "RegionId", "regionId"),
        "RoomId": _BASE._str_field(payload, "RoomId", "roomId"),
        "Result": result,
        "RecRoomId": _BASE._int_field(payload, "RecRoomId", "recRoomId", default=0),
        "MasterPlayerId": _BASE._int_field(payload, "MasterPlayerId", "masterPlayerId", default=0),
    }
    # Patch only this field because joins and heartbeats overlap the callback.
    _patch_player_state(
        player,
        context,
        set_values={"last_game_join_result": last_game_join_result},
    )
    return Response(status_code=204)


def _global_groups(context) -> list[dict[str, Any]]:
    groups = _BASE._get_json_setting(context, GROUPS_SETTING, [])
    return [group for group in groups if isinstance(group, dict)] if isinstance(groups, list) else []


def _save_global_groups(context, groups: list[dict[str, Any]]) -> None:
    _BASE._set_json_setting(context, GROUPS_SETTING, groups[-500:])


def _normalize_group(group: dict[str, Any]) -> dict[str, Any]:
    memberships = [
        {
            "GroupId": int(member.get("GroupId", group.get("GroupId", 0)) or 0),
            "PlayerId": int(member.get("PlayerId", 0) or 0),
            "Permissions": int(member.get("Permissions", 0) or 0),
        }
        for member in group.get("Members", []) if isinstance(member, dict)
    ]
    return {
        "GroupId": int(group.get("GroupId", 0) or 0),
        "Name": str(group.get("Name") or ""),
        "Description": str(group.get("Description") or ""),
        "CreatedAt": str(group.get("CreatedAt") or datetime.now(timezone.utc).isoformat()),
        "ImageName": str(group.get("ImageName") or ""),
        "BanStatus": int(group.get("BanStatus", 0) or 0),
        "CreatorId": int(group.get("CreatorId", 0) or 0),
        "NumMembers": len(memberships),
        "Members": memberships,
    }


async def _handle_group_memberships(player_id: int, request: Request, context) -> Response:
    _authenticated_player(request, context)
    if _find_player_by_legacy_id_25april2019(context, player_id) is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    memberships = []
    for group in _global_groups(context):
        normalized = _normalize_group(group)
        memberships.extend(
            member for member in normalized["Members"] if int(member["PlayerId"]) == player_id
        )
    return JSONResponse(memberships)


async def _handle_create_group(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    player_id = _legacy_id_for_player(player)
    payload = await _BASE._parse_client_payload(request)
    name = _BASE._str_field(payload, "Name", "name").strip()
    description = _BASE._str_field(payload, "Description", "description").strip()
    image_name = _BASE._str_field(payload, "ImageName", "imageName").strip()
    if not name or len(name) > 40:
        return JSONResponse({"Status": 6, "Group": None})
    if len(description) > 500:
        return JSONResponse({"Status": 7, "Group": None})
    name = _filter_user_text(
        context,
        name,
        policy="censor",
        field_context="group.name",
        player=player,
    )
    description = _filter_user_text(
        context,
        description,
        policy="censor",
        field_context="group.description",
        player=player,
    )
    with _EVENT_LOCK:
        groups = _global_groups(context)
        if any(str(group.get("Name") or "").casefold() == name.casefold() for group in groups):
            return JSONResponse({"Status": 4, "Group": None})
        if any(
            player_id == int(member.get("PlayerId", 0) or 0)
            for group in groups
            for member in group.get("Members", []) if isinstance(member, dict)
        ):
            return JSONResponse({"Status": 1, "Group": None})
        group_id = max((int(group.get("GroupId", 0) or 0) for group in groups), default=1000) + 1
        group = {
            "GroupId": group_id,
            "Name": name,
            "Description": description,
            "CreatedAt": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "ImageName": image_name,
            "BanStatus": 0,
            "CreatorId": player_id,
            "Members": [{"GroupId": group_id, "PlayerId": player_id, "Permissions": 0x7F}],
        }
        groups.append(group)
        _save_global_groups(context, groups)
    return JSONResponse({"Status": 0, "Group": _normalize_group(group)})


async def _handle_get_group(
    request: Request, context, *, group_id: int | None = None, group_name: str | None = None
) -> Response:
    _authenticated_player(request, context)
    group = next(
        (
            item for item in _global_groups(context)
            if (group_id is not None and int(item.get("GroupId", 0) or 0) == group_id)
            or (group_name is not None and str(item.get("Name") or "").casefold() == group_name.casefold())
        ),
        None,
    )
    if group is None:
        raise HTTPException(status_code=404, detail="Group not found.")
    return JSONResponse(_normalize_group(group))


async def _handle_delete_group(group_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    player_id = _legacy_id_for_player(player)
    with _EVENT_LOCK:
        groups = _global_groups(context)
        group = next((item for item in groups if int(item.get("GroupId", 0) or 0) == group_id), None)
        if group is None:
            return JSONResponse({"Status": 8})
        membership = next(
            (
                member for member in group.get("Members", [])
                if isinstance(member, dict) and int(member.get("PlayerId", 0) or 0) == player_id
            ),
            None,
        )
        if membership is None or not (int(membership.get("Permissions", 0) or 0) & 0x02):
            return JSONResponse({"Status": 9})
        _save_global_groups(
            context, [item for item in groups if int(item.get("GroupId", 0) or 0) != group_id]
        )
    return JSONResponse({"Status": 0})


def _official_events(context) -> list[dict[str, Any]]:
    configured = context.get_server_setting("events", [])
    candidates = [item for item in configured if isinstance(item, dict)] if isinstance(configured, list) else []
    candidates.extend(_visible_player_events(context))
    events: list[dict[str, Any]] = []
    seen: set[int] = set()
    for item in candidates:
        event_id = int(item.get("EventId", item.get("PlayerEventId", 0)) or 0)
        if event_id <= 0 or event_id in seen:
            continue
        seen.add(event_id)
        events.append(
            {
                "EventId": event_id,
                "Name": str(item.get("Name") or item.get("Title") or ""),
                "Description": str(item.get("Description") or ""),
                "StartTime": str(item.get("StartTime") or item.get("StartAt") or ""),
                "EndTime": str(item.get("EndTime") or item.get("EndAt") or ""),
                "PosterImageName": str(item.get("PosterImageName") or item.get("ImageName") or ""),
                "CreatorPlayerId": int(item.get("CreatorPlayerId", item.get("CreatorPlayerId", 0)) or 0),
                "Status": int(item.get("Status", -1) if item.get("Status") is not None else -1),
            }
        )
    events.sort(key=lambda item: item["StartTime"])
    return events


def _parse_event_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _event_status(event: dict[str, Any]) -> int:
    now = datetime.now(timezone.utc)
    start = _parse_event_time(event.get("StartTime"))
    end = _parse_event_time(event.get("EndTime"))
    if end is not None and now >= end:
        return 2
    if start is not None and now < start:
        return 1
    return 0


async def _handle_events_list(request: Request, context) -> Response:
    _authenticated_player(request, context)
    events = _official_events(context)
    for event in events:
        event["Status"] = _event_status(event)
    return JSONResponse(events)


async def _handle_event_status(event_id: int, request: Request, context) -> Response:
    _authenticated_player(request, context)
    event = next((item for item in _official_events(context) if int(item["EventId"]) == event_id), None)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found.")
    return JSONResponse({"Status": _event_status(event)})


async def _handle_player_elo_update(request: Request, context) -> Response:
    _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    activity_level = _BASE._str_field(payload, "ActivityLevel", "activityLevel").strip()
    raw_updates = payload.get("PlayerScoreUpdates", payload.get("playerScoreUpdates", []))
    if not activity_level or not isinstance(raw_updates, list) or len(raw_updates) < 2:
        raise HTTPException(status_code=400, detail="ActivityLevel and at least two PlayerScoreUpdates are required.")
    updates: list[dict[str, int]] = []
    for raw in raw_updates:
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="Each PlayerScoreUpdate must be an object.")
        player_id = _BASE._int_field(raw, "PlayerId", "playerId", default=0)
        target = _find_player_by_legacy_id_25april2019(context, player_id)
        if target is None:
            raise HTTPException(status_code=400, detail=f"Player {player_id} does not exist.")
        updates.append(
            {
                "PlayerId": player_id,
                "Team": _BASE._int_field(raw, "Team", "team", default=0),
                "GameScore": _BASE._int_field(raw, "GameScore", "gameScore", default=0),
            }
        )
    team_scores: dict[int, float] = {}
    for team in {item["Team"] for item in updates}:
        scores = [item["GameScore"] for item in updates if item["Team"] == team]
        team_scores[team] = sum(scores) / len(scores)
    ordered_scores = sorted(set(team_scores.values()))
    denominator = max(1, len(ordered_scores) - 1)
    with _PLAYER_STATE_LOCK:
        for update in updates:
            target = _find_player_by_legacy_id_25april2019(context, update["PlayerId"])
            if target is None:
                continue
            state = _player_state(target)
            ratings = state.get("elo_ratings")
            if not isinstance(ratings, dict):
                ratings = {}
            existing = ratings.get(activity_level)
            if not isinstance(existing, dict):
                existing = {"Rating": 1000, "GamesPlayed": 0}
            current_rating = int(existing.get("Rating", 1000) or 1000)
            opponent_ratings = []
            for opponent in updates:
                if opponent["Team"] == update["Team"]:
                    continue
                opponent_player = _find_player_by_legacy_id_25april2019(context, opponent["PlayerId"])
                opponent_state = _player_state(opponent_player) if opponent_player is not None else {}
                opponent_all = opponent_state.get("elo_ratings") if isinstance(opponent_state, dict) else {}
                opponent_entry = opponent_all.get(activity_level) if isinstance(opponent_all, dict) else {}
                opponent_ratings.append(int(opponent_entry.get("Rating", 1000) or 1000) if isinstance(opponent_entry, dict) else 1000)
            opponent_average = sum(opponent_ratings) / len(opponent_ratings) if opponent_ratings else 1000
            expected = 1.0 / (1.0 + (10.0 ** ((opponent_average - current_rating) / 400.0)))
            actual = ordered_scores.index(team_scores[update["Team"]]) / denominator
            new_rating = max(100, int(round(current_rating + (24 * (actual - expected)))))
            ratings[activity_level] = {
                "Rating": new_rating,
                "GamesPlayed": max(0, int(existing.get("GamesPlayed", 0) or 0)) + 1,
                "LastTeam": update["Team"],
                "LastGameScore": update["GameScore"],
            }
            state["elo_ratings"] = ratings
            state["last_elo_update"] = {"ActivityLevel": activity_level, "PlayerScoreUpdates": updates}
            _persist_player_state(target, state, context)
    return Response(status_code=204)


def _rec_royale_progress(total_xp: int) -> dict[str, Any]:
    total_xp = max(0, int(total_xp))
    level = 1
    current_threshold = 0
    while level < 50:
        next_threshold = (level * (level + 1) // 2) * 500
        if total_xp < next_threshold:
            break
        level += 1
        current_threshold = next_threshold
    next_threshold = (level * (level + 1) // 2) * 500 if level < 50 else current_threshold
    rank_names = ("Bronze", "Silver", "Gold", "Diamond", "Master")
    rank_idx = min(len(rank_names) - 1, (level - 1) // 10)
    return {
        "TotalXP": total_xp,
        "Level": level,
        "RankIdx": rank_idx,
        "RankName": rank_names[rank_idx],
        "CurrentLevelXPThreshold": current_threshold,
        "NextLevelXPThreshold": next_threshold,
        "NextLevelAcornReward": 100 + (25 * level) if level < 50 else 0,
    }


async def _handle_rec_royale_progress(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    state = _player_state(player)
    royale = state.get("rec_royale")
    if not isinstance(royale, dict):
        royale = {}
    return JSONResponse(_rec_royale_progress(int(royale.get("TotalXP", 0) or 0)))


async def _handle_rec_royale_match_complete(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    rank = max(1, _BASE._int_field(payload, "Rank", "rank", default=18))
    eliminations = max(0, _BASE._int_field(payload, "NumEliminations", "numEliminations", default=0))
    seconds_alive = max(0, _BASE._int_field(payload, "SecondsAlive", "secondsAlive", default=0))
    walk_game = _bool_value(payload.get("WalkGame", payload.get("walkGame", False)))
    custom_game = _bool_value(payload.get("CustomGame", payload.get("customGame", False)))
    chests = max(0, _BASE._int_field(payload, "ChestsOpened", "chestsOpened", default=0))
    shield = max(0, _BASE._int_field(payload, "ShieldPotionsConsumed", "shieldPotionsConsumed", default=0))
    health = max(0, _BASE._int_field(payload, "HealthPotionsConsumed", "healthPotionsConsumed", default=0))
    air_seconds = max(0, _BASE._int_field(payload, "SecondsInAir", "secondsInAir", default=0))
    awards = {
        "Placement": max(0, 210 - (10 * rank)),
        "Eliminations": eliminations * 25,
        "Survival": min(seconds_alive, 1800) // 10,
        "Exploration": (chests * 10) + ((shield + health) * 5) + (min(air_seconds, 600) // 20),
    }
    if custom_game:
        awards = {name: 0 for name in awards}
    elif walk_game:
        awards = {name: max(0, value // 2) for name, value in awards.items()}
    state = _player_state(player)
    frontier = state.get("frontier_pass")
    if not isinstance(frontier, dict):
        frontier = {}
    base_awarded = sum(awards.values())
    season_boost = _frontier_xp_boost(frontier)
    if base_awarded > 0 and season_boost > 0.0:
        awards["Frontier Pass Boost"] = max(1, int(round(base_awarded * season_boost)))
    awarded = sum(awards.values())
    royale = state.get("rec_royale")
    if not isinstance(royale, dict):
        royale = {}
    total_xp = max(0, int(royale.get("TotalXP", 0) or 0)) + awarded
    royale.update(
        {
            "TotalXP": total_xp,
            "MatchesPlayed": max(0, int(royale.get("MatchesPlayed", 0) or 0)) + 1,
            "LastMatch": {
                "Rank": rank,
                "NumEliminations": eliminations,
                "SecondsAlive": seconds_alive,
                "WalkGame": walk_game,
                "CustomGame": custom_game,
                "ChestsOpened": chests,
                "ShieldPotionsConsumed": shield,
                "HealthPotionsConsumed": health,
                "SecondsInAir": air_seconds,
            },
        }
    )
    state["rec_royale"] = royale
    _persist_player_state(player, state, context)
    progress = _rec_royale_progress(total_xp)
    award_strings = [f"{name}: +{value} XP" for name, value in awards.items() if value > 0]
    return JSONResponse({"XPAwardStrings": award_strings, "TotalXPAwarded": awarded, "NewProgress": [progress]})


async def _handle_disallow_in_app_purchases(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    state = _player_state(player)
    return JSONResponse(bool(state.get("disallow_in_app_purchases", False)))


async def _handle_player_objective_completions(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    records = await _parse_json_list(request, payload_name="Objectives")
    canonical_records = []
    additional_xp = 0
    for record in records:
        objective_type = _BASE._int_field(record, "objectiveType", "ObjectiveType", default=-1)
        if objective_type < 0:
            raise HTTPException(status_code=400, detail="objectiveType is required.")
        extra = max(0, _BASE._int_field(record, "additionalXp", "AdditionalXp", default=0))
        additional_xp += extra
        canonical_records.append(
            {
                "objectiveType": objective_type,
                "additionalXp": extra,
                "inParty": _bool_value(record.get("inParty", record.get("InParty", False))),
            }
        )

    # Persist canonical completions and apply base-plus-additional XP.
    state = _player_state(player)
    state["last_player_objective_completions"] = canonical_records
    totals = state.get("objective_completion_counts")
    if not isinstance(totals, dict):
        totals = {}
    objective_period = _weekly_period(context)
    period_content = objective_period["content"]
    period_key = (
        f"{int(period_content['iso_year']):04d}-"
        f"W{int(period_content['iso_week']):02d}"
    )
    periodic_all = state.get("objective_completion_periodic")
    if not isinstance(periodic_all, dict):
        periodic_all = {}
    periodic = periodic_all.get(period_key)
    if not isinstance(periodic, dict):
        periodic = {}
    for record in canonical_records:
        key = str(int(record["objectiveType"]))
        totals[key] = max(0, int(totals.get(key, 0) or 0)) + 1
        periodic[key] = max(0, int(periodic.get(key, 0) or 0)) + 1
    state["objective_completion_counts"] = totals
    # Retain only the active leaderboard period to bound per-player state.
    state["objective_completion_periodic"] = {period_key: periodic}
    _persist_player_state(player, state, context)
    if canonical_records:
        starting_level = max(
            STARTING_PLAYER_LEVEL,
            min(int(player["canonical_level"] or STARTING_PLAYER_LEVEL), MAX_PLAYER_LEVEL),
        )
        delta_xp = sum(
            _objective_xp_award(starting_level, int(record["additionalXp"]))
            for record in canonical_records
        )
        current_level, current_xp = _player_level_progress(
            _total_player_xp(starting_level, int(player["canonical_xp"] or 0)) + delta_xp
        )
        with context.db.transaction() as conn:
            conn.execute(
                """
                UPDATE players
                SET canonical_xp = ?, canonical_level = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE player_id = ? AND is_coach = 0
                """,
                (current_xp, current_level, player["player_id"]),
            )
        levels_gained = max(0, current_level - starting_level)
        if levels_gained:
            totals["5"] = max(0, int(totals.get("5", 0) or 0)) + levels_gained
            periodic["5"] = max(0, int(periodic.get("5", 0) or 0)) + levels_gained
            state["objective_completion_counts"] = totals
            state["objective_completion_periodic"] = {period_key: periodic}
            _persist_player_state(player, state, context)
        await _broadcast_profile_update(_legacy_id_for_player(player), context)
    return Response(status_code=204)


async def _handle_storefront_objective_completions(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    records = await _parse_json_list(request, payload_name="Objectives")
    state = _player_state(player)
    progress = state.get("storefront_objective_progress")
    if not isinstance(progress, dict):
        progress = {}
    for record in records:
        objective_type = _BASE._int_field(record, "objectiveType", "ObjectiveType", default=-1)
        if objective_type < 0:
            raise HTTPException(status_code=400, detail="objectiveType is required.")
        try:
            completion = float(record.get("completionPercentage", record.get("CompletionPercentage", 0.0)))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="completionPercentage must be numeric.") from exc
        completion = max(0.0, min(1.0, completion))
        room_id = record.get("roomId", record.get("RoomId"))
        key = f"{objective_type}:{'' if room_id is None else int(room_id)}"
        prior = progress.get(key)
        prior_completion = float(prior.get("completionPercentage", 0.0)) if isinstance(prior, dict) else 0.0
        progress[key] = {
            "objectiveType": objective_type,
            "completionPercentage": max(prior_completion, completion),
            "roomId": int(room_id) if room_id is not None else None,
        }
    state["storefront_objective_progress"] = progress
    _persist_player_state(player, state, context)
    return Response(status_code=204)


async def _handle_storefront_balance(currency_type: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    if currency_type not in {1, 2, 100, 101, 200}:
        raise HTTPException(status_code=404, detail="Unknown currency type.")
    state = _player_state(player)
    platform = int(state.get("platform", 0) or 0)
    balances = state.get("storefront_balances")
    if not isinstance(balances, dict):
        balances = {}
    try:
        default_balance = TOKEN_BALANCE if currency_type == 2 else 0
        balance = max(0, int(balances.get(str(currency_type), default_balance) or 0))
    except (TypeError, ValueError):
        balance = TOKEN_BALANCE if currency_type == 2 else 0
    return JSONResponse([{"Balance": balance, "CurrencyType": currency_type, "Platform": platform}])


async def _handle_modify_storefront_balance(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    currency_type = _BASE._int_field(payload, "CurrencyType", "currencyType", default=0)
    raw_adds = payload.get("BalanceAdds", payload.get("balanceAdds", []))
    if currency_type not in {1, 2, 100, 101, 200} or not isinstance(raw_adds, list):
        raise HTTPException(status_code=400, detail="Valid CurrencyType and BalanceAdds are required.")
    state = _player_state(player)
    platform = int(state.get("platform", 0) or 0)
    balances = state.get("storefront_balances")
    if not isinstance(balances, dict):
        balances = {}
    current_balance = max(
        0,
        int(balances.get(str(currency_type), TOKEN_BALANCE if currency_type == 2 else 0) or 0),
    )
    today = datetime.now(timezone.utc).date().isoformat()
    rate_state = state.get("storefront_reward_limits")
    if not isinstance(rate_state, dict) or rate_state.get("date") != today:
        rate_state = {"date": today, "counts": {}}
    counts = rate_state.get("counts")
    if not isinstance(counts, dict):
        counts = {}
        rate_state["counts"] = counts
    rate_limits = {
        1: 50, 10: 10, 11: 1, 100: 3, 101: 1, 200: 50, 250: 50,
        303: 10, 1000: 50, 1001: 50, 1002: 10, 1003: 50,
        1100: 10, 1200: 1,
    }
    updates: list[dict[str, Any]] = []
    granted_updates: list[dict[str, Any]] = []
    for raw_add in raw_adds:
        if not isinstance(raw_add, dict):
            raise HTTPException(status_code=400, detail="Every BalanceAdds entry must be an object.")
        add_type = _BASE._int_field(raw_add, "BalanceAddType", "balanceAddType", default=0)
        if add_type not in STOREFRONT_BALANCE_AWARDS:
            updates.append({"UpdateResponse": 1, "Data": None})
            continue
        try:
            multiplier = float(raw_add.get("Multiplier", raw_add.get("multiplier", 1.0)))
        except (TypeError, ValueError):
            multiplier = 0.0
        multiplier = max(0.0, min(multiplier, 10.0))
        rate_limit = int(rate_limits.get(add_type, 1))
        current_count = int(counts.get(str(add_type), 0) or 0)
        if current_count >= rate_limit:
            reward = {
                "BalanceAddType": add_type,
                "BaseAward": 0,
                "BonusAward": 0,
                "RateLimit": rate_limit,
                "CurrentCount": current_count,
                "Total": 0,
                "Platform": platform,
                "BalanceInGiftBox": add_type == 2,
            }
            updates.append({"UpdateResponse": 1, "Data": reward})
            continue
        configured_award = int(STOREFRONT_BALANCE_AWARDS[add_type])
        base_award = (
            _large_token_award("storefront-balance-add", add_type)
            if configured_award > 0
            else 0
        )
        total = max(0, int(round(base_award * multiplier)))
        current_balance += total
        current_count += 1
        counts[str(add_type)] = current_count
        reward = {
            "BalanceAddType": add_type,
            "BaseAward": base_award,
            "BonusAward": 0,
            "RateLimit": rate_limit,
            "CurrentCount": current_count,
            "Total": total,
            "Platform": platform,
            "BalanceInGiftBox": add_type == 2,
        }
        updates.append({"UpdateResponse": 0, "Data": reward})
        granted_updates.append(reward)
    balances[str(currency_type)] = current_balance
    state["storefront_balances"] = balances
    state["storefront_reward_limits"] = rate_state
    _persist_player_state(player, state, context)
    response = {
        "Balance": current_balance,
        "CurrencyType": currency_type,
        "Platform": platform,
        "BalanceUpdates": updates,
    }
    player_id = _legacy_id_for_player(player)
    if granted_updates:
        # Notification 60 uses the same complete balance-update DTO as the
        # HTTP response, including every BalanceUpdates entry.
        await _send_hub_notification(player_id, 60, response, context=context)
    await _send_hub_notification(
        player_id,
        61,
        {"Balance": current_balance, "CurrencyType": currency_type, "Platform": platform},
        context=context,
    )
    return JSONResponse(response)


def _commerce_catalog(context) -> list[dict[str, Any]]:
    # Deliberately local-only: no paid platform products are exposed.
    return [dict(item) for item in COMMERCE_CATALOG]


async def _handle_initiate_commerce_purchase(request: Request, context) -> Response:
    """Reject platform-commerce purchases because this adapter exposes no SKUs."""
    _authenticated_player(request, context)
    raise HTTPException(status_code=404, detail="SKU not found.")


async def _handle_finish_commerce_purchase(
    request: Request, context, *, status: str
) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    transaction_id = _BASE._int_field(
        payload, "transactionId", "TransactionId", default=0
    )
    if transaction_id <= 0:
        raise HTTPException(status_code=400, detail="transactionId is required.")
    with context.db.transaction() as conn:
        row = conn.execute(
            "SELECT state_json FROM player_version_state WHERE player_id = ? AND api_version = ?",
            (player["player_id"], API_VERSION),
        ).fetchone()
        try:
            state = json.loads(row["state_json"] or "{}") if row is not None else {}
        except Exception:
            state = {}
        if not isinstance(state, dict):
            state = {}
        transactions = state.get("commerce_transactions")
        if not isinstance(transactions, list):
            transactions = []
        matched = None
        for transaction in transactions:
            if (
                isinstance(transaction, dict)
                and int(transaction.get("TransactionId") or 0) == transaction_id
            ):
                matched = transaction
                break
        if matched is None:
            raise HTTPException(status_code=404, detail="Purchase transaction not found.")
        old_status = str(matched.get("Status") or "")
        if old_status not in {"initiated", status}:
            raise HTTPException(status_code=409, detail="Purchase transaction is already finalized.")
        if old_status == "initiated":
            matched["Status"] = status
            matched["UpdatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        state["commerce_transactions"] = transactions[-32:]
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), player["player_id"], API_VERSION),
        )
    return Response(status_code=204)


async def _handle_cleanup_commerce_purchases(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    state = _player_state(player)
    transactions = state.get("commerce_transactions")
    if not isinstance(transactions, list):
        transactions = []
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    changed = False
    for transaction in transactions:
        if isinstance(transaction, dict) and str(transaction.get("Status") or "") == "initiated":
            transaction["Status"] = "cancelled"
            transaction["UpdatedAt"] = now
            changed = True
    if changed:
        state["commerce_transactions"] = transactions[-32:]
        _persist_player_state(player, state, context)
    return Response(status_code=204)


def _storefront_gift_drop(
    gift_drop_id: int,
    *,
    friendly_name: str,
    content: int,
    avatar_item_desc: str | None = None,
    avatar_item_type: int | None = None,
    equipment_prefab_name: str | None = None,
    equipment_modification_guid: str | None = None,
    consumable_item_desc: str | None = None,
    rarity: int = 0,
) -> dict[str, Any]:
    return {
        "GiftDropId": gift_drop_id,
        "Level": 0,
        "FriendlyName": friendly_name,
        "Tooltip": friendly_name,
        # StoreScreen requires empty strings instead of null catalog fields.
        "ConsumableItemDesc": consumable_item_desc or "",
        "AvatarItemDesc": avatar_item_desc or "",
        "AvatarItemType": avatar_item_type,
        "EquipmentPrefabName": equipment_prefab_name or "",
        "EquipmentModificationGuid": equipment_modification_guid or "",
        "IsQuery": False,
        "Unique": content in {1, 2},
        "Rarity": rarity,
        "Content": content,
        "Context": 0,
    }


def _storefront_items(
    storefront_type: int,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    drops: list[dict[str, Any]] = []
    # GiftRarity is sparse: Common=0, Uncommon=10, Rare=20, Epic=30, Legendary=50.
    rarity_values = (0, 10, 20, 30, 50)
    if storefront_type == 2:
    # Rotate premium hats, shirts, and wrist-category gloves once per UTC day.
        candidates = [
            (index, desc, friendly_name)
            for index, ((desc, wardrobe_category), friendly_name) in enumerate(
                zip(BUILD_AVATAR_ITEMS, BUILD_AVATAR_ITEM_NAMES), start=1
            )
            if wardrobe_category in {0, 101}
            or (
                wardrobe_category == 200
                and ("glove" in friendly_name.casefold() or "gauntlet" in friendly_name.casefold())
            )
        ]
        current = now or datetime.now(timezone.utc)
        offset = current.date().toordinal() % len(candidates)
        selected = (candidates + candidates)[offset:offset + 12]
        for index, desc, friendly_name in selected:
            drops.append(_storefront_gift_drop(
                100_000 + index,
                friendly_name=friendly_name,
                content=1,
                avatar_item_desc=_normalize_avatar_item_desc(desc),
                avatar_item_type=0,
                rarity=50,
            ))

    # RRO boards expose embedded assets priced in their activity currency.
    themed_keywords = {
        1: ("laser tag",),
        100: ("pirate", "swashbuckler"),
        101: ("dracula", "vampire", "gothic", "bat"),
        200: ("cowboy", "ranger", "frontier", "space marine"),
        400: ("paintball",),
        500: ("bowling",),
    }
    keywords = themed_keywords.get(storefront_type)
    if keywords:
        for index, ((desc, _wardrobe_category), friendly_name) in enumerate(
            zip(BUILD_AVATAR_ITEMS, BUILD_AVATAR_ITEM_NAMES), start=1
        ):
            folded_name = friendly_name.casefold()
            if not any(keyword in folded_name for keyword in keywords):
                continue
            drops.append(_storefront_gift_drop(
                100_000 + index,
                friendly_name=friendly_name,
                content=1,
                avatar_item_desc=_normalize_avatar_item_desc(desc),
                avatar_item_type=0,
                rarity=rarity_values[(index - 1) % len(rarity_values)],
            ))

    # Include activity-specific equipment modifications on RRO boards.
    equipment_store_terms = {
        200: ("recroyale",),
        400: ("paintball",),
    }
    equipment_terms = equipment_store_terms.get(storefront_type, ())
    if equipment_terms:
        for index, (skin_asset_name, (prefab, modification_guid)) in enumerate(
            zip(BUILD_EQUIPMENT_SKIN_ASSET_NAMES, BUILD_EQUIPMENT_SKINS), start=1
        ):
            equipment_identity = f"{skin_asset_name} {prefab}".casefold()
            if not any(term in equipment_identity for term in equipment_terms):
                continue
            drops.append(_storefront_gift_drop(
                200_000 + index,
                friendly_name=_equipment_skin_friendly_name(skin_asset_name, prefab),
                content=2,
                equipment_prefab_name=prefab,
                equipment_modification_guid=modification_guid,
                rarity=rarity_values[(index - 1) % len(rarity_values)],
            ))

    if storefront_type == 3:
        for index, ((desc, _wardrobe_category), friendly_name) in enumerate(
            zip(BUILD_AVATAR_ITEMS, BUILD_AVATAR_ITEM_NAMES), start=1
        ):
            drops.append(_storefront_gift_drop(
                100_000 + index,
                friendly_name=friendly_name,
                content=1,
                avatar_item_desc=_normalize_avatar_item_desc(desc),
                avatar_item_type=0,
                rarity=rarity_values[(index - 1) % len(rarity_values)],
            ))
        for index, (skin_asset_name, (prefab, modification_guid)) in enumerate(
            zip(BUILD_EQUIPMENT_SKIN_ASSET_NAMES, BUILD_EQUIPMENT_SKINS), start=1
        ):
            drops.append(_storefront_gift_drop(
                200_000 + index,
                friendly_name=_equipment_skin_friendly_name(skin_asset_name, prefab),
                content=2,
                equipment_prefab_name=prefab,
                equipment_modification_guid=modification_guid,
                rarity=rarity_values[(index - 1) % len(rarity_values)],
            ))

    if storefront_type in {3, 300}:
        for index, (friendly_name, desc, category) in enumerate(BUILD_CONSUMABLES, start=1):
            if storefront_type == 300 and category != 4:
                continue
            drops.append(_storefront_gift_drop(
                300_000 + index,
                friendly_name=friendly_name,
                content=4,
                consumable_item_desc=desc,
                rarity=rarity_values[(index - 1) % len(rarity_values)],
            ))

    currency_type = STOREFRONT_CURRENCY_TYPES.get(storefront_type)
    if currency_type is None:
        return []
    price_base = {
        1: 500,
        2: 100,
        100: 100,
        101: 100,
        200: 100,
    }[currency_type]
    price_step = 100 if currency_type == 1 else 25 if currency_type != 2 else 50
    return [
        {
            "PurchasableItemId": drop["GiftDropId"],
            "Type": 0,
            "Prices": [{"CurrencyType": currency_type, "Price": price_base + ((index % 6) * price_step)}],
            "IsFeatured": index < 12,
            "GiftDrops": [drop],
        }
        for index, drop in enumerate(drops)
    ]


STOREFRONT_SCHEDULE_KEY = "storefront_gift_drops"
STOREFRONT_SCHEDULE_ANCHOR_UTC = datetime(2019, 4, 25, tzinfo=timezone.utc)


def _storefront_period(context) -> dict[str, Any]:
    expected_rec_center_candidates = sum(
        1
        for (desc, wardrobe_category), friendly_name in zip(
            BUILD_AVATAR_ITEMS,
            BUILD_AVATAR_ITEM_NAMES,
        )
        if wardrobe_category in {0, 101}
        or (
            wardrobe_category == 200
            and (
                "glove" in friendly_name.casefold()
                or "gauntlet" in friendly_name.casefold()
            )
        )
    )
    rec_center_rotation: list[dict[str, Any]] = []
    seen_item_ids: set[int] = set()
    for day_offset in range(max(1, expected_rec_center_candidates)):
        daily_items = _storefront_items(
            2,
            now=STOREFRONT_SCHEDULE_ANCHOR_UTC + timedelta(days=day_offset),
        )
        for item in daily_items:
            item_id = int(item["PurchasableItemId"])
            if item_id in seen_item_ids:
                continue
            seen_item_ids.add(item_id)
            rec_center_rotation.append(item)
        if len(rec_center_rotation) >= expected_rec_center_candidates:
            break
    static_storefronts = {
        str(storefront_type): _storefront_items(
            storefront_type,
            now=STOREFRONT_SCHEDULE_ANCHOR_UTC,
        )
        for storefront_type in sorted(STOREFRONT_CURRENCY_TYPES)
        if storefront_type != 2
    }
    metadata = {
        "strategy": "rotating_window_mapping",
        "base": {"storefronts": static_storefronts},
        "mapping_key": "storefronts",
        "entry_key": "2",
        "candidates": rec_center_rotation,
        "window_size": min(12, len(rec_center_rotation)),
        # The candidate list begins with the anchor day's selected window, so
        # canonical period zero begins at candidate zero.
        "offset_bias": 0,
    }
    catalog_revision = hashlib.sha256(
        json.dumps(metadata, sort_keys=True).encode("utf-8")
    ).hexdigest()[:24]
    context.ensure_anchored_schedule(
        schedule_key=STOREFRONT_SCHEDULE_KEY,
        anchor_utc=STOREFRONT_SCHEDULE_ANCHOR_UTC,
        interval_seconds=24 * 60 * 60,
        catalog_revision=catalog_revision,
        metadata=metadata,
    )
    return context.reconcile_registered_period(
        schedule_key=STOREFRONT_SCHEDULE_KEY,
        now_utc=datetime.now(timezone.utc),
    )


def _current_storefront_items(
    storefront_type: int,
    period: dict[str, Any],
) -> list[dict[str, Any]]:
    storefronts = period["content"].get("storefronts")
    items = (
        storefronts.get(str(storefront_type))
        if isinstance(storefronts, dict)
        else None
    )
    if not isinstance(items, list):
        return []
    return [dict(item) for item in items if isinstance(item, dict)]


async def _handle_gift_drop_storefront(storefront_type: int, request: Request, context) -> Response:
    _authenticated_player(request, context)
    period = _storefront_period(context)
    return JSONResponse({
        "StorefrontType": storefront_type,
        "NextUpdate": str(period["ends_at_utc"]),
        "StoreItems": _current_storefront_items(storefront_type, period),
    })


def _gift_context_value(value: Any) -> int:
    if isinstance(value, str):
        names = {
            "None": -1,
            "Default": 0,
            "Purchased_Gift_A": 500,
            "Purchased_Gift_B": 501,
            "Purchased_Gift_C": 502,
            "Purchased_Gift_D": 503,
        }
        if value in names:
            return names[value]
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


async def _handle_purchase_storefront_item(request: Request, context) -> Response:
    buyer = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    storefront_type = _payload_enum(
        payload,
        "StorefrontType",
        "storefrontType",
        names=STOREFRONT_TYPE_NAMES,
        default=0,
    )
    item_id = _BASE._int_field(payload, "PurchasableItemId", "purchasableItemId", default=0)
    currency_type = _payload_enum(
        payload,
        "CurrencyType",
        "currencyType",
        names=CURRENCY_TYPE_NAMES,
        default=2,
    )
    period = _storefront_period(context)
    item = next(
        (candidate for candidate in _current_storefront_items(storefront_type, period)
         if int(candidate["PurchasableItemId"]) == item_id),
        None,
    )
    if item is None:
        raise HTTPException(status_code=404, detail="Storefront item not found.")
    price_option = next(
        (
            option for option in item.get("Prices", [])
            if int(option.get("CurrencyType", 0) or 0) == currency_type
        ),
        None,
    )
    if price_option is None:
        raise HTTPException(status_code=400, detail="CurrencyType does not match this storefront item.")

    gift_request = payload.get("Gift") if isinstance(payload.get("Gift"), dict) else None
    recipient = buyer
    if gift_request:
        to_player_id = _BASE._int_field(gift_request, "ToPlayerId", "toPlayerId", default=0)
        found = _find_player_by_legacy_id_25april2019(context, to_player_id)
        if found is None:
            raise HTTPException(status_code=404, detail="Gift recipient not found.")
        recipient = found

    price = int(price_option["Price"])
    buyer_state = _player_state(buyer)
    balances = buyer_state.get("storefront_balances")
    if not isinstance(balances, dict):
        balances = {}
    balance_key = str(currency_type)
    current_balance = max(
        0,
        int(balances.get(balance_key, TOKEN_BALANCE if currency_type == 2 else 0) or 0),
    )
    drop = item["GiftDrops"][0]
    item_key = f"{API_VERSION}:store:{item_id}"

    purchased_consumable: dict[str, Any] | None = None
    with context.db.transaction() as conn:
        existing = conn.execute(
            "SELECT quantity, state_json, created_at FROM inventory_items WHERE player_id = ? AND item_key = ?",
            (recipient["player_id"], item_key),
        ).fetchone()
        if bool(drop["Unique"]) and existing is not None and int(existing["quantity"]) > 0:
            return JSONResponse({
                "Balance": current_balance,
                "CurrencyType": currency_type,
                "Platform": int(buyer_state.get("platform", 0) or 0),
                "BalanceUpdates": [{"UpdateResponse": 3, "Data": []}],
            })
        if current_balance < price:
            return JSONResponse({
                "Balance": current_balance,
                "CurrencyType": currency_type,
                "Platform": int(buyer_state.get("platform", 0) or 0),
                "BalanceUpdates": [{"UpdateResponse": 2, "Data": []}],
            })

        existing_quantity = int(existing["quantity"] or 0) if existing is not None else 0
        inventory_state: dict[str, Any]
        if drop["Content"] == 1:
            quantity = existing_quantity + 1
            inventory_state = {
                "AvatarItemDesc": drop["AvatarItemDesc"],
                "AvatarItemType": drop["AvatarItemType"],
            }
        elif drop["Content"] == 2:
            quantity = existing_quantity + 1
            inventory_state = {
                "PrefabName": drop["EquipmentPrefabName"],
                "ModificationGuid": drop["EquipmentModificationGuid"],
            }
        else:
            category = next(
                (category for _, desc, category in BUILD_CONSUMABLES
                 if desc == drop["ConsumableItemDesc"]),
                0,
            )
            limit_count, limit_type = BUILD_CONSUMABLE_LIMITS.get(category, (1, 0))
            try:
                existing_state = json.loads(existing["state_json"] or "{}") if existing is not None else {}
            except Exception:
                existing_state = {}
            if not isinstance(existing_state, dict):
                existing_state = {}
            existing_quantity, existing_state = _settle_realtime_consumable(
                existing_state, existing_quantity
            )
            quantity = existing_quantity + limit_count
            inventory_state = {
                **existing_state,
                "Id": item_id,
                "ConsumableItemDesc": drop["ConsumableItemDesc"],
                "Category": category,
                "PlatformMask": -1,
                "InitialCount": quantity,
                "LimitCount": limit_count,
                "LimitType": limit_type,
                "UnlockedLevel": 0,
                "IsActive": _bool_value(existing_state.get("IsActive")),
                "ActiveDurationMinutes": (
                    max(0, int(existing_state.get("ActiveDurationMinutes", 0) or 0))
                    if limit_type == 2
                    else None
                ),
            }
        conn.execute(
            """
            INSERT INTO inventory_items(player_id, item_key, quantity, state_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(player_id, item_key) DO UPDATE SET
                quantity = excluded.quantity, state_json = excluded.state_json,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            """,
            (recipient["player_id"], item_key, quantity, json.dumps(inventory_state, sort_keys=True)),
        )
        if drop["Content"] not in {1, 2}:
            inventory_row = conn.execute(
                """
                SELECT quantity, created_at
                FROM inventory_items
                WHERE player_id = ? AND item_key = ?
                """,
                (recipient["player_id"], item_key),
            ).fetchone()
            if inventory_row is not None:
                purchased_consumable = _serialize_consumable_inventory_entry(
                    inventory_state,
                    quantity=int(inventory_row["quantity"] or 0),
                    created_at=str(inventory_row["created_at"]),
                )

    new_balance = current_balance - price
    balances[balance_key] = new_balance
    buyer_state["storefront_balances"] = balances
    _persist_player_state(buyer, buyer_state, context)
    gift_id = secrets.randbelow(8_000_000_000_000_000) + 1_000_000_000_000_000 if gift_request else item_id
    gift_context = _gift_context_value(gift_request.get("GiftContext", 0)) if gift_request else 0
    anonymous = _bool_value(gift_request.get("Anonymous", False)) if gift_request else False
    gift_message = (
        _BASE._str_field(gift_request, "Message", "message", default="").strip()
        if gift_request
        else ""
    )
    if len(gift_message) > 512:
        raise HTTPException(status_code=400, detail="Gift message must not exceed 512 characters.")
    if gift_message:
        gift_message = _filter_user_text(
            context,
            gift_message,
            policy="censor",
            field_context="gift.message",
            player=buyer,
        )
    gift_package = {
        "Id": gift_id,
        "FromPlayerId": _legacy_id_for_player(buyer) if gift_request and not anonymous else None,
        "ConsumableItemDesc": drop["ConsumableItemDesc"],
        "AvatarItemType": drop["AvatarItemType"],
        "AvatarItemDesc": drop["AvatarItemDesc"],
        "EquipmentPrefabName": drop["EquipmentPrefabName"],
        "EquipmentModificationGuid": drop["EquipmentModificationGuid"],
        "CurrencyType": 0,
        "Currency": 0,
        "Xp": 0,
        "Level": 0,
        "GiftContext": gift_context,
        "GiftRarity": int(drop["Rarity"]),
        # Use stock gift text only when the sender provides none.
        "Message": gift_message or PURCHASE_GIFT_MESSAGE,
        "Platform": int(buyer_state.get("platform", 0) or 0),
        "Consumed": False,
        "IsValid": True,
        "ErrorMessage": "",
        "SupportsCurrentPlatform": True,
    }
    if gift_request:
        with context.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO gift_boxes(gift_box_id, player_id, state_json, opened, created_at, updated_at)
                VALUES (?, ?, ?, 0, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                """,
                (str(gift_id), recipient["player_id"], json.dumps(gift_package, sort_keys=True)),
            )
        # Purchased gifts require the immediate notification variant.
        await _send_hub_notification(to_player_id, 31, gift_package, context=context)
    if purchased_consumable is not None:
        await _send_hub_notification(
            _legacy_id_for_player(recipient),
            70,
            purchased_consumable,
            context=context,
        )
    return JSONResponse({
        "Balance": new_balance,
        "CurrencyType": currency_type,
        "Platform": int(buyer_state.get("platform", 0) or 0),
        "BalanceUpdates": [{"UpdateResponse": 0, "Data": [gift_package]}],
    })


def _frontier_reward_assets() -> list[tuple[int, str, str]]:
    """Return only exact embedded cowboy/ranger/scout wardrobe assets."""
    approved_names = (
        "Cowboy Hairy",
        "Ranger",
        "Scout Garrison",
        "Scout Raccoon",
        "Cowboy",
        "Cowboy Scarf",
        "Scout Sash",
        "Cowboy Torso",
        "Ranger",
        "Scout Neckerchief",
    )
    assets_by_name: dict[str, list[tuple[int, str, str]]] = {}
    for index, ((desc, _category), friendly_name) in enumerate(
        zip(BUILD_AVATAR_ITEMS, BUILD_AVATAR_ITEM_NAMES), start=1
    ):
        assets_by_name.setdefault(friendly_name, []).append(
            (index, _normalize_avatar_item_desc(desc), friendly_name)
        )

    selected: list[tuple[int, str, str]] = []
    name_occurrences: dict[str, int] = {}
    for friendly_name in approved_names:
        occurrence = name_occurrences.get(friendly_name, 0)
        matches = assets_by_name.get(friendly_name, [])
        if occurrence >= len(matches):
            raise RuntimeError(
                f"Missing exact Frontier wardrobe asset {friendly_name!r} "
                f"occurrence {occurrence + 1}."
            )
        selected.append(matches[occurrence])
        name_occurrences[friendly_name] = occurrence + 1
    return selected


def _frontier_tiers() -> list[dict[str, Any]]:
    assets = _frontier_reward_assets()
    tiers: list[dict[str, Any]] = []
    for tier_number in range(1, FRONTIER_TIER_COUNT + 1):
        paid_asset = assets[tier_number - 1]
        paid_drop = _storefront_gift_drop(
            100_000 + paid_asset[0],
            friendly_name=paid_asset[2],
            content=1,
            avatar_item_desc=paid_asset[1],
            avatar_item_type=0,
            rarity=(0, 10, 20, 30, 50)[(tier_number - 1) % 5],
        )
        free_rewards: list[dict[str, Any]] = []
        xp_boost = float(FRONTIER_XP_BOOSTS.get(tier_number, 0.0))
        if xp_boost > 0.0:
            # TierRewardSpawner has a dedicated xpBoostVisualPrefab for a
            # reward whose GiftDrop is null and SeasonXpBoost is non-zero.
            free_rewards.append({
                "RequiresEliteUpgrade": False,
                "GiftDrop": None,
                "SeasonXpBoost": xp_boost,
            })
        rewards = [
            *free_rewards,
            {
                "RequiresEliteUpgrade": True,
                "GiftDrop": paid_drop,
                "SeasonXpBoost": 0.0,
            },
        ]
        tiers.append({
            "PurchasableItemId": 600_000 + tier_number,
            "Type": 1,
    # Progression uses acorns; Buy Tier uses Rec Center tokens.
            "Prices": [
                {
                    "CurrencyType": FRONTIER_ACORN_CURRENCY_TYPE,
                    "Price": FRONTIER_ACORNS_PER_TIER,
                },
                {
                    "CurrencyType": FRONTIER_PURCHASE_CURRENCY_TYPE,
                    "Price": FRONTIER_TIER_TOKEN_PRICE,
                },
            ],
            "IsFeatured": tier_number == 1,
            # Exact 25 April 2019 PurchasableSeasonTier wire keys. This client
            # splits the single Rewards list locally by RequiresEliteUpgrade.
            "Tier": tier_number,
            "Rewards": rewards,
        })
    return tiers


def _frontier_state(player: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    state = _player_state(player)
    frontier = state.get("frontier_pass")
    if not isinstance(frontier, dict):
        frontier = {}
    frontier = {
        "tier": max(0, min(FRONTIER_TIER_COUNT, int(frontier.get("tier", 0) or 0))),
        "elite": bool(frontier.get("elite", False)),
        "free_claimed": sorted({
            int(value) for value in frontier.get("free_claimed", [])
            if str(value).isdigit() and 0 < int(value) <= FRONTIER_TIER_COUNT
        }),
        "paid_claimed": sorted({
            int(value) for value in frontier.get("paid_claimed", [])
            if str(value).isdigit() and 0 < int(value) <= FRONTIER_TIER_COUNT
        }),
        "modified_at": str(
            frontier.get("modified_at") or "2019-04-25T23:23:01Z"
        ),
    }
    return state, frontier


def _frontier_xp_boost(frontier: dict[str, Any]) -> float:
    """Return the cumulative unlocked SeasonXpBoost from the visible pass."""
    unlocked_tier = max(
        0, min(FRONTIER_TIER_COUNT, int(frontier.get("tier", 0) or 0))
    )
    return sum(
        float(boost)
        for tier_number, boost in FRONTIER_XP_BOOSTS.items()
        if tier_number <= unlocked_tier
    )


def _frontier_season_dto(player: dict[str, Any]) -> dict[str, Any]:
    _state, frontier = _frontier_state(player)
    return {
        "StorefrontType": FRONTIER_STOREFRONT_TYPE,
        "NextUpdate": "2099-12-31T23:59:59Z",
        "Season": 1,
        "Name": "Frontier Pass",
        "StartAt": "2019-04-25T00:00:00Z",
        "EndAt": "2099-12-31T23:59:59Z",
        # CurrencyType.RecRoyale_Season1 (200) is the dedicated Frontier Pass
        # currency in this exact client; RecCenterTokens is value 2.
        "CurrencyType": FRONTIER_ACORN_CURRENCY_TYPE,
        "EliteUpgrade": {
            "PurchasableItemId": FRONTIER_ELITE_ITEM_ID,
            "Type": 2,
            # Elite-track purchase uses Rec Center tokens, not progression acorns.
            "Prices": [
                {
                    "CurrencyType": FRONTIER_PURCHASE_CURRENCY_TYPE,
                    "Price": FRONTIER_ELITE_PRICE,
                }
            ],
            "IsFeatured": True,
        },
        "Tiers": _frontier_tiers(),
        "PersonalDetails": {
            "HasEliteUpgrade": bool(frontier["elite"]),
            "HasEliteUpgradePlatformMask": -1 if frontier["elite"] else 0,
            "CurrentSeasonTier": int(frontier["tier"]),
            "ModifiedAt": str(frontier["modified_at"]),
        },
    }


def _frontier_gift_package(
    drop: dict[str, Any], player: dict[str, Any]
) -> dict[str, Any]:
    return {
        "Id": int(drop["GiftDropId"]),
        "FromPlayerId": None,
        "ConsumableItemDesc": str(drop.get("ConsumableItemDesc") or ""),
        "AvatarItemType": drop.get("AvatarItemType"),
        "AvatarItemDesc": str(drop.get("AvatarItemDesc") or ""),
        "EquipmentPrefabName": str(drop.get("EquipmentPrefabName") or ""),
        "EquipmentModificationGuid": str(drop.get("EquipmentModificationGuid") or ""),
        "CurrencyType": 0,
        "Currency": 0,
        "Xp": 0,
        "Level": 0,
        "GiftContext": 0,
        "GiftRarity": int(drop.get("Rarity", 0) or 0),
        "Message": PURCHASE_GIFT_MESSAGE,
        "Platform": int(_player_state(player).get("platform", 0) or 0),
        "Consumed": False,
        "IsValid": True,
        "ErrorMessage": "",
        "SupportsCurrentPlatform": True,
    }


def _grant_frontier_drop(
    player: dict[str, Any], drop: dict[str, Any], context
) -> dict[str, Any]:
    """Unlock one exact season asset idempotently and return its gift DTO."""
    item_key = f"{API_VERSION}:store:{int(drop['GiftDropId'])}"
    with context.db.transaction() as conn:
        existing = conn.execute(
            "SELECT quantity FROM inventory_items WHERE player_id = ? AND item_key = ?",
            (player["player_id"], item_key),
        ).fetchone()
        if existing is None or int(existing["quantity"] or 0) <= 0:
            inventory_state = {
                "AvatarItemDesc": str(drop.get("AvatarItemDesc") or ""),
                "AvatarItemType": drop.get("AvatarItemType"),
            }
            conn.execute(
                """
                INSERT INTO inventory_items(
                    player_id, item_key, quantity, state_json, created_at, updated_at
                ) VALUES (
                    ?, ?, 1, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                )
                ON CONFLICT(player_id, item_key) DO UPDATE SET
                    quantity = 1, state_json = excluded.state_json,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                """,
                (player["player_id"], item_key, json.dumps(inventory_state, sort_keys=True)),
            )
    return _frontier_gift_package(drop, player)


def _frontier_purchase_response(
    player: dict[str, Any], currency_type: int, balance: int, update_response: int,
    packages: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "Balance": balance,
        "CurrencyType": currency_type,
        "Platform": int(_player_state(player).get("platform", 0) or 0),
        "BalanceUpdates": [{"UpdateResponse": update_response, "Data": packages}],
    }


async def _handle_frontier_season(
    storefront_type: int, request: Request, context
) -> Response:
    player = _authenticated_player(request, context)
    if storefront_type != FRONTIER_STOREFRONT_TYPE:
        raise HTTPException(status_code=404, detail="Storefront season not found.")
    return JSONResponse(_frontier_season_dto(player))


async def _handle_buy_frontier_tier(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    storefront_type = _payload_enum(
        payload, "StorefrontType", "storefrontType",
        names=STOREFRONT_TYPE_NAMES, default=0,
    )
    item_id = _BASE._int_field(payload, "PurchasableItemId", "purchasableItemId", default=0)
    currency_type = _payload_enum(
        payload, "CurrencyType", "currencyType",
        names=CURRENCY_TYPE_NAMES, default=0,
    )
    if (
        storefront_type != FRONTIER_STOREFRONT_TYPE
        or currency_type
        not in {FRONTIER_ACORN_CURRENCY_TYPE, FRONTIER_PURCHASE_CURRENCY_TYPE}
    ):
        raise HTTPException(
            status_code=400,
            detail="Frontier Pass tiers require acorns or Rec Center tokens.",
        )

    with _PLAYER_STATE_LOCK:
        current = _find_player_by_legacy_id_25april2019(
            context, _legacy_id_for_player(player)
        ) or player
        state, frontier = _frontier_state(current)
        next_tier = int(frontier["tier"]) + 1
        if next_tier > FRONTIER_TIER_COUNT or item_id != 600_000 + next_tier:
            raise HTTPException(status_code=409, detail="Frontier Pass tiers must be purchased in order.")
        balances = state.get("storefront_balances")
        if not isinstance(balances, dict):
            balances = {}
        default_balance = TOKEN_BALANCE if currency_type == FRONTIER_PURCHASE_CURRENCY_TYPE else 0
        balance = max(0, int(balances.get(str(currency_type), default_balance) or 0))
        tier_price = (
            FRONTIER_ACORNS_PER_TIER
            if currency_type == FRONTIER_ACORN_CURRENCY_TYPE
            else FRONTIER_TIER_TOKEN_PRICE
        )
        if balance < tier_price:
            return JSONResponse(
                _frontier_purchase_response(current, currency_type, balance, 2, [])
            )

        tier = _frontier_tiers()[next_tier - 1]
        tier_rewards = list(tier["Rewards"])
        free_rewards = [
            reward
            for reward in tier_rewards
            if not bool(reward.get("RequiresEliteUpgrade", False))
        ]
        paid_rewards = [
            reward
            for reward in tier_rewards
            if bool(reward.get("RequiresEliteUpgrade", False))
        ]
        rewards = [*free_rewards, *(paid_rewards if bool(frontier["elite"]) else [])]
        packages = [
            _grant_frontier_drop(current, reward["GiftDrop"], context)
            for reward in rewards
            if isinstance(reward.get("GiftDrop"), dict)
        ]
        if free_rewards:
            frontier["free_claimed"] = sorted({*frontier["free_claimed"], next_tier})
        if bool(frontier["elite"]):
            frontier["paid_claimed"] = sorted({*frontier["paid_claimed"], next_tier})
        frontier["tier"] = next_tier
        frontier["modified_at"] = _format_recnet_datetime(datetime.now(timezone.utc))
        balance -= tier_price
        balances[str(currency_type)] = balance
        state["storefront_balances"] = balances
        state["frontier_pass"] = frontier
        _persist_player_state(current, state, context)

    await _send_hub_notification(
        _legacy_id_for_player(current), 61,
        {
            "Balance": balance,
            "CurrencyType": currency_type,
            "Platform": int(state.get("platform", 0) or 0),
        },
        context=context,
    )
    return JSONResponse(
        _frontier_purchase_response(current, currency_type, balance, 0, packages)
    )


async def _handle_buy_frontier_elite(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    storefront_type = _payload_enum(
        payload, "StorefrontType", "storefrontType",
        names=STOREFRONT_TYPE_NAMES, default=0,
    )
    item_id = _BASE._int_field(payload, "PurchasableItemId", "purchasableItemId", default=0)
    currency_type = _payload_enum(
        payload, "CurrencyType", "currencyType",
        names=CURRENCY_TYPE_NAMES, default=0,
    )
    if (
        storefront_type != FRONTIER_STOREFRONT_TYPE
        or item_id != FRONTIER_ELITE_ITEM_ID
        or currency_type != FRONTIER_PURCHASE_CURRENCY_TYPE
    ):
        raise HTTPException(status_code=400, detail="Invalid Frontier Pass elite purchase.")

    with _PLAYER_STATE_LOCK:
        current = _find_player_by_legacy_id_25april2019(
            context, _legacy_id_for_player(player)
        ) or player
        state, frontier = _frontier_state(current)
        balances = state.get("storefront_balances")
        if not isinstance(balances, dict):
            balances = {}
        balance = max(
            0,
            int(
                balances.get(
                    str(FRONTIER_PURCHASE_CURRENCY_TYPE), TOKEN_BALANCE
                )
                or 0
            ),
        )
        if bool(frontier["elite"]):
            return JSONResponse(
                _frontier_purchase_response(
                    current, FRONTIER_PURCHASE_CURRENCY_TYPE, balance, 3, []
                )
            )
        if balance < FRONTIER_ELITE_PRICE:
            return JSONResponse(
                _frontier_purchase_response(
                    current, FRONTIER_PURCHASE_CURRENCY_TYPE, balance, 2, []
                )
            )

        tiers = _frontier_tiers()
        already_claimed = set(frontier["paid_claimed"])
        claim_tiers = [
            tier_number for tier_number in range(1, int(frontier["tier"]) + 1)
            if tier_number not in already_claimed
        ]
        packages = [
            _grant_frontier_drop(current, reward["GiftDrop"], context)
            for tier_number in claim_tiers
            for reward in tiers[tier_number - 1]["Rewards"]
            if bool(reward.get("RequiresEliteUpgrade", False))
            and isinstance(reward.get("GiftDrop"), dict)
        ]
        frontier["elite"] = True
        frontier["paid_claimed"] = sorted({*already_claimed, *claim_tiers})
        frontier["modified_at"] = _format_recnet_datetime(datetime.now(timezone.utc))
        balance -= FRONTIER_ELITE_PRICE
        balances[str(FRONTIER_PURCHASE_CURRENCY_TYPE)] = balance
        state["storefront_balances"] = balances
        state["frontier_pass"] = frontier
        _persist_player_state(current, state, context)

    await _send_hub_notification(
        _legacy_id_for_player(current), 61,
        {
            "Balance": balance,
            "CurrencyType": FRONTIER_PURCHASE_CURRENCY_TYPE,
            "Platform": int(state.get("platform", 0) or 0),
        },
        context=context,
    )
    return JSONResponse(
        _frontier_purchase_response(
            current, FRONTIER_PURCHASE_CURRENCY_TYPE, balance, 0, packages
        )
    )


# Version-owned weekly challenge pool.
_WEEKLY_CHALLENGE_DATA = load_version_json(API_VERSION, "weekly_challenges.json", list)
WEEKLY_CHALLENGE_POOL = tuple(
    {
        **item,
        "room_locations": tuple(int(value) for value in item["room_locations"]),
    }
    for item in _WEEKLY_CHALLENGE_DATA
)

WEEKLY_LOCATION_BY_SCENE_GUID = load_version_json(
    API_VERSION, "weekly_location_scene_guids.json", dict
)
# v2 is the authoritative weekly progress ledger.
WEEKLY_SCHEDULE_KEY = "weekly_challenges_v2"
WEEKLY_SCHEDULE_ANCHOR_UTC = datetime(1970, 1, 5, tzinfo=timezone.utc)
WEEKLY_SCHEDULE_INTERVAL_SECONDS = 7 * 24 * 60 * 60


def _weekly_period(context, *, next_week: bool = False) -> dict[str, Any]:
    reward_catalog = [
        {"prefab": prefab, "modification_guid": modification_guid}
        for prefab, modification_guid in BUILD_EQUIPMENT_SKINS
        if prefab != "[MakerPen]"
    ]
    metadata = {
        "strategy": "deterministic_pool_slots",
        "slot_count": 3,
        "pool": [dict(item) for item in WEEKLY_CHALLENGE_POOL],
        "reward_catalog": reward_catalog,
    }
    catalog_revision = hashlib.sha256(
        json.dumps(metadata, sort_keys=True).encode("utf-8")
    ).hexdigest()[:24]
    context.ensure_anchored_schedule(
        schedule_key=WEEKLY_SCHEDULE_KEY,
        anchor_utc=WEEKLY_SCHEDULE_ANCHOR_UTC,
        interval_seconds=WEEKLY_SCHEDULE_INTERVAL_SECONDS,
        catalog_revision=catalog_revision,
        metadata=metadata,
    )
    target_time = datetime.now(timezone.utc)
    if next_week:
        target_time += timedelta(days=7)
    return context.reconcile_registered_period(
        schedule_key=WEEKLY_SCHEDULE_KEY,
        now_utc=target_time,
    )


def _weekly_count_config(
    *,
    event_type: int,
    required_count: int,
    room_locations: tuple[int, ...],
    current_count: int = 0,
    complete: bool = False,
) -> str:
    current_count = max(0, min(int(current_count), int(required_count)))
    complete = bool(complete or current_count >= required_count)
    definition = {
    # Compact keys match the nested challenge serialization contract.
        "ct": 1,
        "c": complete,
        "ipc": True,
        "wc": [
            {
                "ct": 7,
                "c": False,
                "ipc": True,
                "vs": [{"l": int(location)} for location in room_locations],
                "in": True,
                "ex": True,
            }
        ],
        "ctc": [
            {
                "ct": 6,
                "c": False,
                "ipc": True,
                "vs": [int(event_type)],
                "in": True,
                "ex": True,
            }
        ],
        "t": int(required_count),
        "cc": current_count,
    }
    return json.dumps(definition, separators=(",", ":"))


async def _current_weekly_room_location(player, context) -> int | None:
    session = await _active_game_session_for_player(player, context)
    if not isinstance(session, dict):
        return None
    location_id = str(session.get("RoomSceneLocationId") or "").casefold()
    return WEEKLY_LOCATION_BY_SCENE_GUID.get(location_id)


def _weekly_progress_state(
    player,
    context,
    *,
    period_id: str,
    map_id: int,
) -> tuple[dict[str, Any], bool]:
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT state_json, reward_claimed
            FROM timed_content_player_progress
            WHERE player_id = ? AND schedule_key = ? AND period_id = ?
            """,
            (player["player_id"], WEEKLY_SCHEDULE_KEY, period_id),
        ).fetchone()
    if row is None:
        legacy_weekly = _player_state(player).get("weekly_challenges")
        legacy_progress = (
            legacy_weekly.get(str(map_id))
            if isinstance(legacy_weekly, dict)
            else None
        )
        if not isinstance(legacy_progress, dict):
            return {}, False
        return dict(legacy_progress), bool(legacy_progress.get("reward_granted", False))
    try:
        progress = json.loads(row["state_json"] or "{}")
    except Exception:
        progress = {}
    return (
        progress if isinstance(progress, dict) else {},
        bool(row["reward_claimed"]),
    )


def _weekly_challenge_map(
    player,
    context,
    *,
    next_week: bool = False,
) -> tuple[dict[str, Any], tuple[str, str]]:
    now = datetime.now(timezone.utc)
    period = _weekly_period(context, next_week=next_week)
    content = period["content"]
    week_start = datetime.fromisoformat(
        str(period["starts_at_utc"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    week_end = datetime.fromisoformat(
        str(period["ends_at_utc"]).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    map_id = int(content["map_id"])
    iso_year = int(content["iso_year"])
    iso_week = int(content["iso_week"])
    reward = content["reward"]
    prefab = str(reward["prefab"])
    modification_guid = str(reward["modification_guid"])
    specs = content["slots"]
    saved, _ = (
        ({}, False)
        if next_week
        else _weekly_progress_state(
            player,
            context,
            period_id=str(period["period_id"]),
            map_id=map_id,
        )
    )
    saved_challenges = saved.get("challenges")
    if not isinstance(saved_challenges, dict):
        saved_challenges = {}
    challenges: list[dict[str, Any]] = []
    for slot, spec in enumerate(specs):
        challenge_id = (map_id * 10) + slot
        saved_challenge = saved_challenges.get(str(challenge_id))
        if not isinstance(saved_challenge, dict):
            saved_challenge = {}
        current_count = max(0, int(saved_challenge.get("current_count", 0) or 0))
        complete = bool(saved_challenge.get("complete", False))
        challenges.append(
            {
                "ChallengeId": challenge_id,
                "Name": spec["name"],
                "Config": _weekly_count_config(
                    event_type=spec["event_type"],
                    required_count=spec["count"],
                    room_locations=spec["room_locations"],
                    current_count=current_count,
                    complete=complete,
                ),
                "Description": spec["description"],
                "Tooltip": f"{spec['description']} Complete all 3 weekly objectives to earn the skin.",
                "Complete": complete,
                "Slot": slot,
            }
        )
    gift = {
        "GiftDropId": map_id,
        "ConsumableItemDesc": None,
        "AvatarItemDesc": None,
        "AvatarItemType": None,
        "EquipmentPrefabName": prefab,
        "EquipmentModificationGuid": modification_guid,
        "StorefrontType": 3,
        "Xp": 0,
        "Level": 0,
        "GiftContext": 4,
        "GiftRarity": 20,
    }
    challenge_map = {
        "ChallengeMapId": map_id,
        "ChallengeThemeString": f"Weekly Challenge {iso_year}-W{iso_week:02d}",
        "ChallengeThemeId": None,
        "Challenges": challenges,
        "Gifts": [gift],
        "StartAt": week_start.isoformat().replace("+00:00", "Z"),
        "EndAt": week_end.isoformat().replace("+00:00", "Z"),
        "ServerTime": now.isoformat().replace("+00:00", "Z"),
        "CompleteAll": all(challenge["Complete"] for challenge in challenges),
    }
    return challenge_map, (prefab, modification_guid)


def _weekly_challenge_response(player, context, *, next_week: bool = False) -> dict[str, Any]:
    challenge_map, _ = _weekly_challenge_map(player, context, next_week=next_week)
    # ChallengeMapResponse deserializes the map from Message.
    return {"Success": True, "Message": json.dumps(challenge_map, separators=(",", ":"))}


async def _grant_weekly_skin_reward(
    player,
    *,
    map_id: int,
    prefab: str,
    modification_guid: str,
    context,
) -> None:
    legacy_player_id = _legacy_id_for_player(player)
    gift_id = (map_id * 100_000) + legacy_player_id
    gift_package = {
        "Id": gift_id,
        "FromPlayerId": None,
        "ConsumableItemDesc": None,
        "AvatarItemType": None,
        "AvatarItemDesc": None,
        "EquipmentPrefabName": prefab,
        "EquipmentModificationGuid": modification_guid,
        "CurrencyType": 0,
        "Currency": 0,
        "Xp": 0,
        "Level": 0,
        "GiftContext": 4,
        "GiftRarity": 20,
        "Message": "Weekly Challenge complete!",
        "Platform": int(_player_state(player).get("platform", 0) or 0),
        "Consumed": False,
        "IsValid": True,
        "ErrorMessage": "",
        "SupportsCurrentPlatform": True,
    }
    inserted_gift = False
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO inventory_items(player_id, item_key, quantity, state_json, created_at, updated_at)
            VALUES (?, ?, 1, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(player_id, item_key) DO UPDATE SET
                quantity = 1, state_json = excluded.state_json,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            """,
            (
                player["player_id"],
                f"{API_VERSION}:weekly:{map_id}",
                json.dumps(
                    {"PrefabName": prefab, "ModificationGuid": modification_guid},
                    sort_keys=True,
                ),
            ),
        )
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO gift_boxes(
                gift_box_id, player_id, state_json, opened, created_at, updated_at
            )
            VALUES (?, ?, ?, 0, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            """,
            (
                str(gift_id),
                player["player_id"],
                json.dumps(gift_package, sort_keys=True),
            ),
        )
        inserted_gift = cursor.rowcount > 0
    if inserted_gift:
        await _send_hub_notification(
            legacy_player_id,
            30,
            gift_package,
            context=context,
        )


def _record_weekly_progress(
    *,
    player,
    period_id: str,
    challenge_id: int,
    incoming_count: int,
    current_map: dict[str, Any],
    context,
) -> bool:
    challenge_by_id = {
        int(challenge["ChallengeId"]): challenge
        for challenge in current_map["Challenges"]
    }
    required_counts = {
        challenge_key: int(json.loads(str(challenge["Config"]))["t"])
        for challenge_key, challenge in challenge_by_id.items()
    }
    required_count = required_counts[challenge_id]
    with context.db.transaction() as conn:
        row = conn.execute(
            """
            SELECT state_json, reward_claimed
            FROM timed_content_player_progress
            WHERE player_id = ? AND schedule_key = ? AND period_id = ?
            """,
            (player["player_id"], WEEKLY_SCHEDULE_KEY, period_id),
        ).fetchone()
        if row is None:
            legacy_weekly = _player_state(player).get("weekly_challenges")
            legacy_progress = (
                legacy_weekly.get(str(current_map["ChallengeMapId"]))
                if isinstance(legacy_weekly, dict)
                else None
            )
            progress = dict(legacy_progress) if isinstance(legacy_progress, dict) else {}
            reward_claimed = bool(progress.pop("reward_granted", False))
        else:
            try:
                progress = json.loads(row["state_json"] or "{}")
            except Exception:
                progress = {}
            if not isinstance(progress, dict):
                progress = {}
            reward_claimed = bool(row["reward_claimed"])
        progress_challenges = progress.get("challenges")
        if not isinstance(progress_challenges, dict):
            progress_challenges = {}
        previous = progress_challenges.get(str(challenge_id))
        if not isinstance(previous, dict):
            previous = {}
        current_count = max(
            int(previous.get("current_count", 0) or 0),
            min(max(0, int(incoming_count)), required_count),
        )
        complete = (
            bool(previous.get("complete", False))
            or current_count >= required_count
        )
        if complete:
            current_count = required_count
        progress_challenges[str(challenge_id)] = {
            "current_count": current_count,
            "complete": complete,
        }
        progress["challenges"] = progress_challenges
        completed_ids = {
            int(raw_id)
            for raw_id, saved_challenge in progress_challenges.items()
            if str(raw_id).isdigit()
            and int(raw_id) in required_counts
            and isinstance(saved_challenge, dict)
            and bool(saved_challenge.get("complete", False))
            and int(saved_challenge.get("current_count", 0) or 0)
            >= required_counts[int(raw_id)]
        }
        grant_reward = completed_ids == set(required_counts) and not reward_claimed
        conn.execute(
            """
            INSERT INTO timed_content_player_progress(
                player_id, schedule_key, period_id, state_json, reward_claimed,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ON CONFLICT(player_id, schedule_key, period_id) DO UPDATE SET
                state_json = excluded.state_json,
                reward_claimed = MAX(
                    timed_content_player_progress.reward_claimed,
                    excluded.reward_claimed
                ),
                updated_at = excluded.updated_at
            """,
            (
                player["player_id"],
                WEEKLY_SCHEDULE_KEY,
                period_id,
                json.dumps(progress, sort_keys=True),
                int(reward_claimed),
            ),
        )
    return grant_reward


async def _handle_update_weekly_challenge_progress(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    map_id = _BASE._int_field(payload, "ChallengeMapId", "challengeMapId", default=0)
    challenge_id = _BASE._int_field(payload, "ChallengeId", "challengeId", default=0)
    current_map, reward_skin = _weekly_challenge_map(player, context)
    if map_id != int(current_map["ChallengeMapId"]):
        raise HTTPException(status_code=409, detail="Challenge map is not current.")
    challenge_by_id = {
        int(challenge["ChallengeId"]): challenge for challenge in current_map["Challenges"]
    }
    challenge = challenge_by_id.get(challenge_id)
    if challenge is None:
        raise HTTPException(status_code=404, detail="Challenge not found.")
    try:
        authoritative_config = json.loads(str(challenge["Config"]))
        required_count = int(authoritative_config["t"])
        allowed_locations = {
            int(value["l"])
            for value in authoritative_config.get("wc", [])[0].get("vs", [])
            if isinstance(value, dict) and str(value.get("l", "")).lstrip("-").isdigit()
        }
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=500,
            detail="The current challenge snapshot is invalid.",
        ) from exc
    raw_config = payload.get("Config", payload.get("config"))
    try:
        incoming_config = json.loads(raw_config) if isinstance(raw_config, str) else raw_config
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Config must be valid challenge JSON.") from exc
    if not isinstance(incoming_config, dict):
        raise HTTPException(status_code=400, detail="Config must be a challenge object.")
    incoming_count = max(
        0,
        min(
            int(incoming_config.get("cc", 0) or 0),
            required_count,
        ),
    )
    # Accept GameEnd progress only for the player's authoritative room gate.
    if await _current_weekly_room_location(player, context) not in allowed_locations:
        return Response(status_code=204)

    current_period = _weekly_period(context)
    if int(current_period["content"]["map_id"]) != map_id:
        raise HTTPException(status_code=409, detail="Challenge period changed; refresh it.")
    period_id = str(current_period["period_id"])
    grant_reward = _record_weekly_progress(
        player=player,
        period_id=period_id,
        challenge_id=challenge_id,
        incoming_count=incoming_count,
        current_map=current_map,
        context=context,
    )
    if grant_reward:
        await _grant_weekly_skin_reward(
            player,
            map_id=map_id,
            prefab=reward_skin[0],
            modification_guid=reward_skin[1],
            context=context,
        )
        with context.db.transaction() as conn:
            conn.execute(
                """
                UPDATE timed_content_player_progress
                SET reward_claimed = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE player_id = ? AND schedule_key = ? AND period_id = ?
                """,
                (player["player_id"], WEEKLY_SCHEDULE_KEY, period_id),
            )
    return Response(status_code=204)


async def _handle_balance_add_config(
    balance_add_type: int, currency_type: int, request: Request, context
) -> Response:
    _authenticated_player(request, context)
    configured_award = int(STOREFRONT_BALANCE_AWARDS.get(balance_add_type, 0))
    base_award = (
        _large_token_award("storefront-balance-add", balance_add_type)
        if configured_award > 0
        else 0
    )
    return JSONResponse({
        "CurrencyType": currency_type,
        "BalanceAddType": balance_add_type,
        "RateLimitType": 0,
        "BaseAward": base_award,
        "BonusAwardMin": 0,
        "BonusAwardMax": 0,
        "RateLimit": 1 if base_award > 0 else 0,
        "MaxPartialMultiplier": 1.0,
        "IgnorePartialMultiplier": True,
        "BalanceInGiftBox": False,
    })


async def _handle_invention_list(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    player_id = _legacy_id_for_player(player)
    records = _BASE._get_json_setting(context, INVENTIONS_SETTING, [])
    if not isinstance(records, list):
        records = []
    inventions = [
        item.get("Invention")
        for item in records
        if isinstance(item, dict) and isinstance(item.get("Invention"), dict)
        and not context.is_content_quarantined(
            "invention",
            int(item["Invention"].get("InventionId") or 0),
        )
    ]
    records_by_id = {
        int(item["Invention"].get("InventionId") or 0): item
        for item in records
        if isinstance(item, dict) and isinstance(item.get("Invention"), dict)
    }
    if request.url.path.casefold().rstrip("/").endswith("/mine"):
        downloaded = _BASE._get_json_setting(
            context,
            _canonical_player_setting_key("downloaded_inventions", player["player_id"]),
            [],
        )
        downloaded_ids = {
            int(value) for value in downloaded if str(value).isdigit()
        } if isinstance(downloaded, list) else set()
        inventions = [
            item
            for item in inventions
            if int(item.get("CreatorPlayerId") or 0) == player_id
            or int(item.get("InventionId") or 0) in downloaded_ids
        ]
    else:
        inventions = [item for item in inventions if bool(item.get("IsPublished", False))]
        search = str(
            request.query_params.get("value")
            or request.query_params.get("SearchTerm")
            or request.query_params.get("searchTerm")
            or ""
        ).strip().casefold()
        if search:
    # Split compact hashtags from free text before searching inventions.
            selected_tags = {
                match.casefold()
                for match in re.findall(r"#([^#\s]+)", search)
                if match
            }
            text_search = re.sub(r"#[^#\s]+", " ", search).strip()
            if selected_tags:
                inventions = [
                    item
                    for item in inventions
                    if selected_tags.issubset({
                        str(tag).strip().lstrip("#").casefold()
                        for tag in [
                            *records_by_id.get(
                                int(item.get("InventionId") or 0), {}
                            ).get("AutoTags", []),
                            *records_by_id.get(
                                int(item.get("InventionId") or 0), {}
                            ).get("PlayerAddedTags", []),
                        ]
                        if str(tag).strip()
                    })
                ]
            if text_search.startswith("@"):
                creator = _find_player_by_username(context, text_search[1:].strip())
                creator_id = _legacy_id_for_player(creator) if creator is not None else 0
                inventions = [
                    item
                    for item in inventions
                    if int(item.get("CreatorPlayerId") or 0) == creator_id
                ]
            elif text_search:
                inventions = [
                    item
                    for item in inventions
                    if text_search in str(item.get("Name") or "").casefold()
                    or text_search in str(item.get("Description") or "").casefold()
                ]
    return JSONResponse(inventions)


def _invention_records(context) -> list[dict[str, Any]]:
    records = _BASE._get_json_setting(context, INVENTIONS_SETTING, [])
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]


def _save_invention_records(context, records: list[dict[str, Any]]) -> None:
    _BASE._set_json_setting(context, INVENTIONS_SETTING, records[-500:])


def _record_for_invention(
    records: list[dict[str, Any]],
    invention_id: int,
    *,
    context=None,
) -> dict[str, Any] | None:
    record = next(
        (
            record
            for record in records
            if isinstance(record.get("Invention"), dict)
            and int(record["Invention"].get("InventionId") or 0) == invention_id
        ),
        None,
    )
    if (
        record is not None
        and context is not None
        and context.is_content_quarantined("invention", invention_id)
    ):
        return None
    return record


def _invention_versions(record: dict[str, Any]) -> list[dict[str, Any]]:
    versions = record.get("Versions")
    if not isinstance(versions, list):
        current = record.get("InventionVersion")
        versions = [current] if isinstance(current, dict) else []
    canonical = [version for version in versions if isinstance(version, dict)]
    canonical.sort(key=lambda item: int(item.get("VersionNumber") or 0))
    return canonical


def _invention_response(
    status: int,
    record: dict[str, Any] | None = None,
    *,
    version: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "Status": int(status),
        "Invention": record.get("Invention") if isinstance(record, dict) else None,
        "InventionVersion": (
            version
            if isinstance(version, dict)
            else record.get("InventionVersion") if isinstance(record, dict) else None
        ),
    }


def _find_invention_record(context, invention_id: int) -> dict[str, Any] | None:
    return _record_for_invention(
        _invention_records(context),
        invention_id,
        context=context,
    )


async def _handle_get_invention(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    invention_id = _BASE._int_field(
        dict(request.query_params), "inventionId", "InventionId", default=0
    )
    record = _find_invention_record(context, invention_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Invention not found.")
    invention = record["Invention"]
    if (
        not bool(invention.get("IsPublished", False))
        and int(invention.get("CreatorPlayerId") or 0) != _legacy_id_for_player(player)
    ):
        raise HTTPException(status_code=404, detail="Invention not found.")
    return JSONResponse(invention)


async def _handle_get_invention_details(request: Request, context) -> Response:
    _authenticated_player(request, context)
    invention_id = _BASE._int_field(
        dict(request.query_params), "inventionId", "InventionId", default=0
    )
    record = _find_invention_record(context, invention_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Invention not found.")
    auto_tags = record.get("AutoTags") if isinstance(record.get("AutoTags"), list) else []
    player_tags = (
        record.get("PlayerAddedTags")
        if isinstance(record.get("PlayerAddedTags"), list)
        else []
    )
    return JSONResponse({
        "Tags": [
            *({"Tag": str(tag).lstrip("#"), "Type": 1} for tag in auto_tags),
            *({"Tag": str(tag).lstrip("#"), "Type": 0} for tag in player_tags),
        ]
    })


async def _handle_get_personal_invention_details(
    invention_id: int, request: Request, context
) -> Response:
    player = _authenticated_player(request, context)
    if _find_invention_record(context, invention_id) is None:
        raise HTTPException(status_code=404, detail="Invention not found.")
    cheered = _BASE._get_json_setting(
        context,
        _canonical_player_setting_key("cheered_inventions", player["player_id"]),
        [],
    )
    cheered_ids = {
        int(value) for value in cheered if str(value).isdigit()
    } if isinstance(cheered, list) else set()
    return JSONResponse({"IsCheering": invention_id in cheered_ids})


async def _handle_get_invention_version(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    query = dict(request.query_params)
    invention_id = _BASE._int_field(query, "inventionId", "InventionId", default=0)
    version_number = _BASE._int_field(query, "version", "Version", default=0)
    record = _find_invention_record(context, invention_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Invention version not found.")
    invention = record["Invention"]
    if (
        not bool(invention.get("IsPublished", False))
        and int(invention.get("CreatorPlayerId") or 0) != _legacy_id_for_player(player)
    ):
        raise HTTPException(status_code=404, detail="Invention version not found.")
    version = next(
        (
            item
            for item in _invention_versions(record)
            if int(item.get("VersionNumber") or 0) == version_number
        ),
        None,
    )
    if version is None:
        raise HTTPException(status_code=404, detail="Invention version not found.")
    return JSONResponse(version)


async def _handle_invention_filters(request: Request, context) -> Response:
    _authenticated_player(request, context)
    counts: dict[str, int] = {}
    for record in _invention_records(context):
        invention = record.get("Invention")
        if not isinstance(invention, dict) or not bool(invention.get("IsPublished", False)):
            continue
        if context.is_content_quarantined(
            "invention",
            int(invention.get("InventionId") or 0),
        ):
            continue
        for key in ("AutoTags", "PlayerAddedTags"):
            tags = record.get(key)
            if not isinstance(tags, list):
                continue
            for raw_tag in tags:
                tag = str(raw_tag).strip().lstrip("#")
                if tag:
                    counts[tag] = counts.get(tag, 0) + 1
    # Pin only tags with icons in InventionFilterButtonIconMap.
    pinned = [
        "art", "character", "costume", "decor",
        "gadget", "environment", "weapon", "sound",
    ]
    defaults = [
        *pinned, "holotar", "large", "medium", "small", "decoration",
        "furniture", "game", "logic", "prop", "vehicle", "music", "interactive",
    ]
    ranked = sorted(counts, key=lambda tag: (-counts[tag], tag.casefold()))
    # Keep icon-backed categories first in the edit dialog's popular tags.
    popular = list(dict.fromkeys([*pinned, *ranked, *defaults]))[:20]
    return JSONResponse({"PinnedFilters": pinned, "PopularFilters": popular})


async def _handle_invention_batch(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    raw_ids = payload.get("InventionIds", payload.get("inventionIds", []))
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="InventionIds must be a list.")
    requested_ids = []
    for raw_id in raw_ids[:100]:
        if str(raw_id).lstrip("-").isdigit() and int(raw_id) > 0:
            invention_id = int(raw_id)
            if invention_id not in requested_ids:
                requested_ids.append(invention_id)
    local_id = _legacy_id_for_player(player)
    results = []
    for invention_id in requested_ids:
        record = _find_invention_record(context, invention_id)
        invention = record.get("Invention") if isinstance(record, dict) else None
        if not isinstance(invention, dict):
            continue
        if bool(invention.get("IsPublished", False)) or int(
            invention.get("CreatorPlayerId") or 0
        ) == local_id:
            results.append(invention)
    return JSONResponse(results)


async def _handle_get_invention_versions(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    invention_id = _BASE._int_field(
        dict(request.query_params), "inventionId", "InventionId", default=0
    )
    record = _find_invention_record(context, invention_id)
    invention = record.get("Invention") if isinstance(record, dict) else None
    if not isinstance(invention, dict):
        raise HTTPException(status_code=404, detail="Invention not found.")
    if (
        not bool(invention.get("IsPublished", False))
        and int(invention.get("CreatorPlayerId") or 0) != _legacy_id_for_player(player)
    ):
        raise HTTPException(status_code=404, detail="Invention not found.")
    return JSONResponse(_invention_versions(record))


async def _handle_update_invention(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    _assert_invention_publishing_allowed(context, player)
    query = dict(request.query_params)
    invention_id = _BASE._int_field(query, "inventionId", "InventionId", default=0)
    with _INVENTION_LOCK:
        records = _invention_records(context)
        record = _record_for_invention(records, invention_id, context=context)
        if record is None:
            return JSONResponse(_invention_response(7))
        invention = record["Invention"]
        if int(invention.get("CreatorPlayerId") or 0) != _legacy_id_for_player(player):
            return JSONResponse(_invention_response(6))

        supplied = [
            key
            for key in ("name", "description", "imgName", "permission")
            if key in query or key.casefold() in {str(value).casefold() for value in query}
        ]
        if len(supplied) != 1:
            return JSONResponse(_invention_response(1))
        field = supplied[0]
        value = next(
            (raw for key, raw in query.items() if str(key).casefold() == field.casefold()),
            "",
        )
        if field == "name":
            name = str(value).strip()
            if len(name) < 3:
                return JSONResponse(_invention_response(4))
            if len(name) > 64:
                return JSONResponse(_invention_response(5))
            name = _filter_user_text(
                context,
                name,
                policy="censor",
                field_context="invention.name",
                player=player,
            )
            if any(
                other is not record
                and isinstance(other.get("Invention"), dict)
                and int(other["Invention"].get("CreatorPlayerId") or 0)
                == _legacy_id_for_player(player)
                and str(other["Invention"].get("Name") or "").casefold() == name.casefold()
                for other in records
            ):
                return JSONResponse(_invention_response(3))
            invention["Name"] = name
        elif field == "description":
            description = str(value).strip()
            if len(description) > 1024:
                return JSONResponse(_invention_response(10))
            description = _filter_user_text(
                context,
                description,
                policy="censor",
                field_context="invention.description",
                player=player,
            )
            invention["Description"] = description
        elif field == "imgName":
            image_name = str(value).strip()
            if not image_name:
                return JSONResponse(_invention_response(8))
            invention["ImageName"] = image_name
        else:
            try:
                permission = int(value)
            except (TypeError, ValueError):
                return JSONResponse(_invention_response(1))
            if permission not in {0, 10, 20, 40, 60, 80, 100}:
                return JSONResponse(_invention_response(1))
            invention["GeneralPermission"] = permission
        invention["ModifiedAt"] = _format_recnet_datetime(datetime.now(timezone.utc))
        _save_invention_records(context, records)
        return JSONResponse(_invention_response(0, record))


async def _handle_set_invention_tags(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    _assert_invention_publishing_allowed(context, player)
    payload = await _BASE._parse_client_payload(request)
    invention_id = _BASE._int_field(payload, "InventionId", "inventionId", default=0)
    auto_tags = payload.get("AutoTags", payload.get("autoTags", []))
    custom_tags = payload.get("CustomTags", payload.get("customTags", []))
    if not isinstance(auto_tags, list) or not isinstance(custom_tags, list):
        return JSONResponse({"Result": 3, "Tags": []})
    normalized_auto = list(dict.fromkeys(
        str(tag).strip().lstrip("#") for tag in auto_tags if str(tag).strip()
    ))
    normalized_custom = list(dict.fromkeys(
        str(tag).strip().lstrip("#") for tag in custom_tags if str(tag).strip()
    ))
    normalized_custom = list(dict.fromkeys(
        _filter_user_text(
            context,
            tag,
            policy="censor",
            field_context="invention.tag",
            player=player,
        )
        for tag in normalized_custom
    ))
    all_tags = [*normalized_auto, *normalized_custom]
    if len(all_tags) > 10:
        return JSONResponse({"Result": 1, "Tags": all_tags[:10]})
    if any(len(tag) > 24 for tag in all_tags):
        return JSONResponse({"Result": 5, "Tags": all_tags})
    with _INVENTION_LOCK:
        records = _invention_records(context)
        record = _record_for_invention(records, invention_id, context=context)
        if record is None:
            return JSONResponse({"Result": 12, "Tags": []})
        if int(record["Invention"].get("CreatorPlayerId") or 0) != _legacy_id_for_player(player):
            return JSONResponse({"Result": 10, "Tags": []})
        record["AutoTags"] = normalized_auto
        record["PlayerAddedTags"] = normalized_custom
        record["Invention"]["ModifiedAt"] = _format_recnet_datetime(datetime.now(timezone.utc))
        _save_invention_records(context, records)
    return JSONResponse({"Result": 0, "Tags": all_tags})


async def _handle_add_invention_version(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    _assert_invention_publishing_allowed(context, player)
    invention_id = _BASE._int_field(
        dict(request.query_params), "inventionId", "InventionId", default=0
    )
    fields = await _parse_multipart_fields(request)
    data = fields.get("data")
    raw_request = fields.get("newVersionRequest")
    if data is None or raw_request is None:
        raise HTTPException(
            status_code=400,
            detail="Multipart fields data and newVersionRequest are required.",
        )
    try:
        version_request = json.loads(raw_request.decode("utf-8", errors="strict"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid newVersionRequest JSON.") from exc
    if not isinstance(version_request, dict):
        raise HTTPException(status_code=400, detail="newVersionRequest must be an object.")

    with _INVENTION_LOCK:
        records = _invention_records(context)
        record = _record_for_invention(records, invention_id, context=context)
        if record is None:
            return JSONResponse(_invention_response(7))
        invention = record["Invention"]
        if int(invention.get("CreatorPlayerId") or 0) != _legacy_id_for_player(player):
            return JSONResponse(_invention_response(6))
        creation_room_id = _BASE._int_field(
            version_request, "creationRoomId", "CreationRoomId", default=0
        )
        active = _player_state(player).get("game_session")
        if creation_room_id > 0 and (
            not isinstance(active, dict) or int(active.get("RoomId") or 0) != creation_room_id
        ):
            return JSONResponse(_invention_response(21))
        versions = _invention_versions(record)
        version_number = max(
            (int(item.get("VersionNumber") or 0) for item in versions), default=0
        ) + 1
        blob_name = (
            f"{API_VERSION}-invention-{invention_id}-v{version_number}-"
            f"{secrets.token_hex(8)}.blob"
        )
        version = {
            "InventionId": invention_id,
            "VersionNumber": version_number,
            "InstantiationCost": max(
                0,
                _BASE._int_field(
                    version_request, "instantiationCost", "InstantiationCost", default=0
                ),
            ),
            "LightsCost": max(
                0,
                _BASE._int_field(version_request, "lightsCost", "LightsCost", default=0),
            ),
            "BlobName": blob_name,
        }
        referenced_images = version_request.get("referencedImages", [])
        if not isinstance(referenced_images, list):
            referenced_images = []
        with context.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO room_data_blobs(
                    blob_name, room_id, owner_player_id, data, image_list_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?,
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                """,
                (
                    blob_name,
                    f"invention:{API_VERSION}:{invention_id}",
                    player["player_id"],
                    data,
                    json.dumps(referenced_images),
                ),
            )
        versions.append(version)
        record["Versions"] = versions
        record["InventionVersion"] = version
        record["ReferencedImages"] = referenced_images
        record["ReferencedData"] = version_request.get("referencedData", [])
        record["ReferencedInventions"] = version_request.get("referencedInventions", [])
        invention["CurrentVersionNumber"] = version_number
        invention["ModifiedAt"] = _format_recnet_datetime(datetime.now(timezone.utc))
        _save_invention_records(context, records)
    return JSONResponse(_invention_response(0, record, version=version))


async def _handle_invention_lifecycle(
    action: str, request: Request, context
) -> Response:
    player = _authenticated_player(request, context)
    if action == "publish":
        _assert_invention_publishing_allowed(context, player)
    query = dict(request.query_params)
    invention_id = _BASE._int_field(query, "inventionId", "InventionId", default=0)
    local_id = _legacy_id_for_player(player)
    with _INVENTION_LOCK:
        records = _invention_records(context)
        record = _record_for_invention(records, invention_id, context=context)
        if record is None:
            return JSONResponse(_invention_response(7))
        invention = record["Invention"]
        is_creator = int(invention.get("CreatorPlayerId") or 0) == local_id

        if action == "delete":
            if not is_creator:
                return JSONResponse(_invention_response(6))
            records = [candidate for candidate in records if candidate is not record]
            _save_invention_records(context, records)
            with context.db.transaction() as conn:
                conn.execute(
                    "DELETE FROM room_data_blobs WHERE room_id = ? AND owner_player_id = ?",
                    (f"invention:{API_VERSION}:{invention_id}", player["player_id"]),
                )
        else:
            if action == "publish":
                if not is_creator:
                    return JSONResponse(_invention_response(6))
                if bool(invention.get("ModerationRestricted", False)):
                    return JSONResponse(_invention_response(21, record))
                if bool(invention.get("IsPublished", False)):
                    return JSONResponse(_invention_response(15, record))
                permission = _BASE._int_field(
                    query, "permissionLevel", "PermissionLevel", default=0
                )
                if permission not in {10, 20, 40, 60, 80, 100}:
                    return JSONResponse(_invention_response(1, record))
                now = _format_recnet_datetime(datetime.now(timezone.utc))
                invention["IsPublished"] = True
                invention["GeneralPermission"] = permission
                invention["FirstPublishedAt"] = invention.get("FirstPublishedAt") or now
                invention["ModifiedAt"] = now
            elif action == "unpublish":
                if not is_creator:
                    return JSONResponse(_invention_response(6))
                if not bool(invention.get("IsPublished", False)):
                    return JSONResponse(_invention_response(16, record))
                invention["IsPublished"] = False
                invention["ModifiedAt"] = _format_recnet_datetime(datetime.now(timezone.utc))
            elif action == "download":
                downloaded_key = _canonical_player_setting_key(
                    "downloaded_inventions", player["player_id"]
                )
                downloaded = _BASE._get_json_setting(context, downloaded_key, [])
                downloaded_ids = {
                    int(value) for value in downloaded if str(value).isdigit()
                } if isinstance(downloaded, list) else set()
                if is_creator or invention_id in downloaded_ids:
                    return JSONResponse(_invention_response(19, record))
                if (
                    not bool(invention.get("IsPublished", False))
                    or int(invention.get("GeneralPermission") or 0) <= 0
                ):
                    return JSONResponse(_invention_response(21, record))
                downloaded_ids.add(invention_id)
                _BASE._set_json_setting(context, downloaded_key, sorted(downloaded_ids))
                invention["NumDownloads"] = max(
                    0, int(invention.get("NumDownloads", 0) or 0) + 1
                )
            else:
                raise HTTPException(status_code=500, detail="Unknown invention action.")
            _save_invention_records(context, records)
        response = JSONResponse(_invention_response(0, record))
    await _broadcast_invention_cache_invalidation(context)
    return response


async def _handle_report_invention(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    invention_id = _BASE._int_field(payload, "InventionId", "inventionId", default=0)
    report_category = _BASE._int_field(
        payload, "ReportCategory", "reportCategory", default=-1
    )
    details = _BASE._str_field(payload, "Details", "details", default="").strip()
    if _find_invention_record(context, invention_id) is None:
        raise HTTPException(status_code=404, detail="Invention not found.")
    if report_category not in {-1, 0, 1, 2, 3, 4}:
        raise HTTPException(status_code=400, detail="Invalid ReportCategory.")
    _submit_canonical_report(
        reporter=player,
        target_type="invention",
        target_id=invention_id,
        raw_category=report_category,
        canonical_category=INVENTION_REPORT_CATEGORY_MAP.get(report_category, "unknown"),
        category_schema="invention_reporting_v1",
        details=details[:2000],
        room_id=None,
        game_session_id=None,
        source_endpoint="api/inventions/v1/report",
        source_payload=payload,
        context=context,
    )
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_cheer_invention(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    invention_id = _BASE._int_field(payload, "InventionId", "inventionId", default=0)
    cheer = _bool_value(payload.get("Cheer", payload.get("cheer", False)))
    key = _canonical_player_setting_key("cheered_inventions", player["player_id"])
    with _INVENTION_LOCK:
        records = _invention_records(context)
        record = _record_for_invention(records, invention_id, context=context)
        if record is None:
            return JSONResponse(_invention_response(7))
        invention = record["Invention"]
        if not bool(invention.get("IsPublished", False)):
            return JSONResponse(_invention_response(21, record))
        cheered = _BASE._get_json_setting(context, key, [])
        cheered_ids = {
            int(value) for value in cheered if str(value).isdigit()
        } if isinstance(cheered, list) else set()
        already_cheering = invention_id in cheered_ids
        if cheer and already_cheering:
            return JSONResponse(_invention_response(23, record))
        if not cheer and not already_cheering:
            return JSONResponse(_invention_response(24, record))
        if cheer:
            cheered_ids.add(invention_id)
        else:
            cheered_ids.discard(invention_id)
        _BASE._set_json_setting(context, key, sorted(cheered_ids))
        invention["CheerCount"] = max(
            0,
            int(invention.get("CheerCount", 0) or 0) + (1 if cheer else -1),
        )
        _save_invention_records(context, records)
        response = JSONResponse(_invention_response(0, record))
    await _broadcast_invention_cache_invalidation(context)
    return response


async def _handle_join_player(request: Request, context) -> Response:
    player = _authenticated_player(
        request, context, allow_account_sanction=True
    )
    payload = await _BASE._parse_client_payload(request)
    local_id = _legacy_id_for_player(player)
    expected_ids = (
        []
        if _player_room_lock_active(player, context)
        else _expected_player_ids(payload, local_id)
    )
    target_player_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    if _player_room_lock_active(player, context) and target_player_id > 0:
        return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
    if target_player_id <= 0:
        # The boot flow emits JoinPlayerRequest(0) as a self/dorm fallback.
        # Returning PlayerNotOnline creates the full-screen error dialog.
        dorm = _ensure_dorm_room(player, context)
        game_session = await _register_dorm_instance(player, dorm, context)
        await _revoke_outgoing_game_invites(local_id, context)
        await _persist_active_game_session(player, game_session, context)
        return JSONResponse({
            "Result": 0,
            "GameSession": game_session,
            "RoomDetails": _serialize_dorm_details(
                dorm, local_player_id=_legacy_id_for_player(player)
            ),
        })
    target = _find_player_by_legacy_id_25april2019(context, target_player_id)
    if target is None:
        return JSONResponse({"Result": 2, "GameSession": None, "RoomDetails": None})
    target_session = await context.require_transient().get_membership(
        target_player_id
    )
    if not isinstance(target_session, dict):
        return JSONResponse({"Result": 2, "GameSession": None, "RoomDetails": None})

    game_session_id = int(target_session.get("GameSessionId") or 0)
    local_session = await context.require_transient().get_membership(local_id)
    local_session_id = (
        int(local_session.get("GameSessionId") or 0)
        if isinstance(local_session, dict)
        else 0
    )
    if game_session_id > 0 and local_session_id == game_session_id:
        return JSONResponse({"Result": 17, "GameSession": None, "RoomDetails": None})
    accepted_inviter_ids: set[int] = set()
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        instance = next(
            (item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id), None
        )
        if instance is None:
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        members = {
            int(value)
            for value in await context.require_transient().session_member_ids(
                game_session_id
            )
            if str(value).lstrip("-").isdigit()
        }
        # Verify current membership before routing JoinPlayer.
        if target_player_id not in members:
            return JSONResponse({"Result": 2, "GameSession": None, "RoomDetails": None})
        if bool(instance.get("Private", False)) and not await _can_enter_private_instance(instance, local_id, context):
            return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
        if local_id in {
            int(value) for value in instance.get("banned", [])
            if str(value).lstrip("-").isdigit()
        }:
            return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
        if len(members) >= int(instance.get("MaxCapacity") or 0) and local_id not in members:
            return JSONResponse({"Result": 3, "GameSession": None, "RoomDetails": None})
        if bool(instance.get("GameInProgress", False)) and not bool(instance.get("supports_join_in_progress", False)):
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        accepted_inviter_ids = await _instance_inviter_ids(instance, local_id, context)
        await _consume_instance_invite(instance, local_id, context)
        _write_game_instances(conn, instances)

    game_session = _public_game_session(
        instance, member_count=len(members) + (0 if local_id in members else 1)
    )
    await _revoke_outgoing_game_invites(local_id, context)
    await _persist_active_game_session(player, game_session, context)
    await _notify_join_companions(
        local_id,
        expected_ids,
        game_session,
        context,
        auto_follow=False,
    )
    for inviter_id in accepted_inviter_ids:
        _remove_recnet_messages(
            player,
            context,
            from_player_id=inviter_id,
            message_types={0},
        )
    record = _find_coach_room_by_id(int(instance["RoomId"]), context)
    if record is not None:
        room_details = _serialize_coach_details(record, context, local_player_id=local_id)
    elif bool(instance.get("is_dorm", False)):
        owner = _find_player_by_legacy_id_25april2019(context, int(instance.get("owner_player_id") or 0))
        if owner is None:
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        room_details = _serialize_dorm_details(
            _ensure_dorm_room(owner, context), local_player_id=local_id
        )
    elif bool(instance.get("is_ugc", False)):
        ugc = _find_ugc_room(context, room_id=int(instance["RoomId"]))
        if ugc is None:
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        room_details = _serialize_ugc_details(ugc, local_player_id=local_id)
    else:
        return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
    return JSONResponse({
        "Result": 0,
        "GameSession": game_session,
        "RoomDetails": room_details,
    })


async def _handle_join_player_event(request: Request, context) -> Response:
    player = _authenticated_player(
        request, context, allow_account_sanction=True
    )
    if _player_room_lock_active(player, context):
        return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
    payload = await _BASE._parse_client_payload(request)
    event_id = _BASE._int_field(payload, "EventId", "eventId", default=0)
    event = _event_by_id(event_id, context)
    if event is None:
        return JSONResponse({"Result": 20, "GameSession": None, "RoomDetails": None})
    now = datetime.now(timezone.utc)
    start = _parse_recnet_datetime(event.get("StartTime"))
    end = _parse_recnet_datetime(event.get("EndTime"))
    if start is None or end is None:
        return JSONResponse({"Result": 20, "GameSession": None, "RoomDetails": None})
    if now < start:
        return JSONResponse({"Result": 4, "GameSession": None, "RoomDetails": None})
    if now >= end:
        return JSONResponse({"Result": 5, "GameSession": None, "RoomDetails": None})
    expected = payload.get("ExpectedPlayerIds", payload.get("expectedPlayerIds", []))
    if not isinstance(expected, list):
        expected = []
    expected_ids = [int(value) for value in expected if str(value).lstrip("-").isdigit()]
    room_id = int(event.get("RoomId", 0) or 0)
    coach = _find_coach_room_by_id(room_id, context)
    ugc = _find_ugc_room(context, room_id=room_id) if coach is None else None
    if coach is None and ugc is None:
        return JSONResponse({"Result": 20, "GameSession": None, "RoomDetails": None})

    if coach is not None:
        coach_asset = coach.get("asset") if isinstance(coach.get("asset"), dict) else {}
        scenes = coach_asset.get("x") if isinstance(coach_asset.get("x"), list) else []
        if not scenes:
            return JSONResponse({"Result": 20, "GameSession": None, "RoomDetails": None})
        game_session = await _join_coach_instance(
            player, coach, scenes[0], context, private=False, invited_player_ids=expected_ids
        )
        room_details = _serialize_coach_details(
            coach, context, local_player_id=_legacy_id_for_player(player)
        )
    else:
        assert ugc is not None
        is_owner = ugc["row"]["owner_player_id"] == player["player_id"]
        if int(ugc["metadata"].get("accessibility", 0) or 0) == 0 and not is_owner:
            return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
        game_session = await _join_ugc_instance(
            player, ugc, context, private=False, invited_player_ids=expected_ids
        )
        refreshed = _find_ugc_room(context, room_id=room_id) or ugc
        room_details = _serialize_ugc_details(
            refreshed, local_player_id=_legacy_id_for_player(player)
        )

    game_session_id = int(game_session.get("GameSessionId", 0) or 0)
    with context.db.transaction() as conn:
        instances = _read_game_instances(conn)
        for instance in instances:
            if int(instance.get("GameSessionId", 0) or 0) == game_session_id:
                instance["PlayerEventId"] = event_id
                break
        _write_game_instances(conn, instances)
    game_session["PlayerEventId"] = event_id
    await _revoke_outgoing_game_invites(
        _legacy_id_for_player(player), context
    )
    await _persist_active_game_session(player, game_session, context)
    await _notify_join_companions(
        _legacy_id_for_player(player),
        expected_ids,
        game_session,
        context,
        auto_follow=False,
    )
    return JSONResponse({"Result": 0, "GameSession": game_session, "RoomDetails": room_details})


async def _handle_join_room(request: Request, context) -> Response:
    player = _authenticated_player(
        request, context, allow_account_sanction=True
    )
    payload = await _BASE._parse_client_payload(request)
    local_id = _legacy_id_for_player(player)
    room_name = _BASE._str_field(payload, "RoomName", "roomName")
    scene_name = _BASE._str_field(payload, "SceneName", "sceneName")
    private = _bool_value(payload.get("Private", payload.get("private", False)))
    room_locked = _player_room_lock_active(player, context)
    if room_locked and room_name.casefold() != "dormroom":
        return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
    expected_ids = (
        []
        if room_locked
        else _expected_player_ids(payload, local_id)
    )
    auto_follow = (
        False if room_locked else _party_auto_follow_requested(payload)
    )
    if room_name.casefold() == "dormroom":
        if scene_name and scene_name.casefold() != "home":
            return JSONResponse({"Result": 20, "GameSession": None, "RoomDetails": None})
        dorm = _ensure_dorm_room(player, context)
        game_session = await _register_dorm_instance(
            player, dorm, context, expected_ids,
        )
        await _revoke_outgoing_game_invites(local_id, context)
        await _persist_active_game_session(player, game_session, context)
        await _notify_join_companions(
            local_id,
            expected_ids,
            game_session,
            context,
            auto_follow=auto_follow,
        )
        return JSONResponse({
            "Result": 0,
            "GameSession": game_session,
            "RoomDetails": _serialize_dorm_details(
                dorm, local_player_id=_legacy_id_for_player(player)
            ),
        })

    record = _find_coach_room_by_name(room_name, context)
    if record is None:
        ugc = _find_ugc_room(context, name=room_name)
        if ugc is None:
            return JSONResponse({"Result": 20, "GameSession": None, "RoomDetails": None})
        is_owner = ugc["row"]["owner_player_id"] == player["player_id"]
        if int(ugc["metadata"].get("accessibility", 0) or 0) == 0 and not is_owner:
            return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
        game_session = await _join_ugc_instance(
            player, ugc, context, private=private,
            invited_player_ids=expected_ids,
        )
        await _revoke_outgoing_game_invites(local_id, context)
        await _persist_active_game_session(player, game_session, context)
        await _notify_join_companions(
            local_id,
            expected_ids,
            game_session,
            context,
            auto_follow=auto_follow,
        )
        refreshed = _find_ugc_room(context, room_id=int(ugc["version"]["room_id"])) or ugc
        return JSONResponse({
            "Result": 0,
            "GameSession": game_session,
            "RoomDetails": _serialize_ugc_details(
                refreshed, local_player_id=_legacy_id_for_player(player)
            ),
        })
    scene = _find_scene(record, scene_name)
    if scene is None:
        return JSONResponse({"Result": 20, "GameSession": None, "RoomDetails": None})
    game_session = await _join_coach_instance(
        player, record, scene, context, private=private,
        invited_player_ids=expected_ids,
    )
    await _revoke_outgoing_game_invites(local_id, context)
    await _persist_active_game_session(player, game_session, context)
    await _notify_join_companions(
        local_id,
        expected_ids,
        game_session,
        context,
        auto_follow=auto_follow,
    )
    return JSONResponse({
        "Result": 0,
        "GameSession": game_session,
        "RoomDetails": _serialize_coach_details(
            record, context, local_player_id=_legacy_id_for_player(player)
        ),
    })


async def _handle_join_instance(request: Request, context) -> Response:
    player = _authenticated_player(
        request, context, allow_account_sanction=True
    )
    if _player_room_lock_active(player, context):
        return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
    payload = await _BASE._parse_client_payload(request)
    local_id = _legacy_id_for_player(player)
    expected_ids = _expected_player_ids(payload, local_id)
    game_session_id = _BASE._int_field(payload, "GameSessionId", "gameSessionId", default=0)
    if game_session_id <= 0:
        return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
    local_session = await context.require_transient().get_membership(local_id)
    local_session_id = (
        int(local_session.get("GameSessionId") or 0)
        if isinstance(local_session, dict)
        else 0
    )
    if local_session_id == game_session_id:
        return JSONResponse({"Result": 17, "GameSession": None, "RoomDetails": None})
    accepted_inviter_ids: set[int] = set()
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        instance = next((item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id), None)
        if instance is None:
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        members = {
            int(value)
            for value in await context.require_transient().session_member_ids(
                game_session_id
            )
            if str(value).lstrip("-").isdigit()
        }
        if bool(instance.get("Private", False)) and not await _can_enter_private_instance(instance, local_id, context):
            return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
        if local_id in _instance_player_ids(instance, "banned"):
            return JSONResponse({"Result": 25, "GameSession": None, "RoomDetails": None})
        if len(members) >= int(instance.get("MaxCapacity") or 0) and local_id not in members:
            return JSONResponse({"Result": 3, "GameSession": None, "RoomDetails": None})
        if bool(instance.get("GameInProgress", False)) and not bool(instance.get("supports_join_in_progress", False)):
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        accepted_inviter_ids = await _instance_inviter_ids(instance, local_id, context)
        await _consume_instance_invite(instance, local_id, context)
        _write_game_instances(conn, instances)
    game_session = _public_game_session(
        instance, member_count=len(members) + (0 if local_id in members else 1)
    )
    await _revoke_outgoing_game_invites(local_id, context)
    await _persist_active_game_session(player, game_session, context)
    await _notify_join_companions(
        local_id,
        expected_ids,
        game_session,
        context,
        auto_follow=False,
    )
    for inviter_id in accepted_inviter_ids:
        _remove_recnet_messages(
            player,
            context,
            from_player_id=inviter_id,
            message_types={0},
        )
    record = _find_coach_room_by_id(int(instance["RoomId"]), context)
    if record is not None:
        room_details = _serialize_coach_details(record, context, local_player_id=local_id)
    elif bool(instance.get("is_dorm", False)):
        owner = _find_player_by_legacy_id_25april2019(context, int(instance.get("owner_player_id") or 0))
        if owner is None:
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        room_details = _serialize_dorm_details(
            _ensure_dorm_room(owner, context), local_player_id=local_id
        )
    elif bool(instance.get("is_ugc", False)):
        ugc = _find_ugc_room(context, room_id=int(instance["RoomId"]))
        if ugc is None:
            return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
        room_details = _serialize_ugc_details(ugc, local_player_id=local_id)
    else:
        return JSONResponse({"Result": 1, "GameSession": None, "RoomDetails": None})
    return JSONResponse({"Result": 0, "GameSession": game_session, "RoomDetails": room_details})


async def _handle_get_rooms_mycreated_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    dorm = _ensure_dorm_room(player, context)
    ugc = [
        _serialize_ugc_room(record)
        for record in _all_ugc_records(context)
        if record["row"]["owner_player_id"] == player["player_id"]
    ]
    return JSONResponse([_serialize_dorm_room(dorm), *ugc])


async def _handle_get_rooms_bookmarked_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("bookmarked_rooms", player["player_id"])
    room_ids = _BASE._get_json_setting(context, key, [])
    if not isinstance(room_ids, list):
        room_ids = []
    result = []
    for value in room_ids:
        try:
            room_id = int(value)
        except (TypeError, ValueError):
            continue
        dorm = _find_dorm_room_by_room_id(context, room_id)
        if dorm is not None:
            result.append(_serialize_dorm_room(dorm))
            continue
        record = _find_coach_room_by_id(room_id, context)
        if record is not None:
            result.append(_serialize_coach_room(record))
            continue
        ugc = _find_ugc_room(context, room_id=room_id)
        if ugc is not None:
            result.append(_serialize_ugc_room(ugc))
    return JSONResponse(result)


async def _handle_get_rooms_recent_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("recent_rooms", player["player_id"])
    room_ids = _BASE._get_json_setting(context, key, [])
    if not isinstance(room_ids, list):
        room_ids = []
    result: list[dict[str, Any]] = []
    for raw_room_id in room_ids:
        if not str(raw_room_id).lstrip("-").isdigit():
            continue
        room_id = int(raw_room_id)
        # The Recent watch page is for activity/player rooms, not DormRoom.
        if _find_dorm_room_by_room_id(context, room_id) is not None:
            continue
        coach = _find_coach_room_by_id(room_id, context)
        if coach is not None:
            result.append(_serialize_coach_room(coach))
            continue
        ugc = _find_ugc_room(context, room_id=room_id)
        if ugc is not None:
            result.append(_serialize_ugc_room(ugc))
    skip = max(0, _BASE._int_field(dict(request.query_params), "skip", "Skip", default=0))
    take = max(0, min(100, _BASE._int_field(dict(request.query_params), "take", "Take", default=10)))
    return JSONResponse(result[skip : skip + take])


async def _handle_get_rooms_subscribed_v2(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    key = _canonical_player_setting_key("player_subscriptions", player["player_id"])
    canonical_ids = _BASE._get_json_setting(context, key, [])
    subscribed = {str(value) for value in canonical_ids} if isinstance(canonical_ids, list) else set()
    result: list[dict[str, Any]] = []
    if COACH_PLAYER_UUID in subscribed:
        result.extend(_serialize_coach_room(_coach_room_record(room)) for room in BUILD_COACH_ROOMS)
    result.extend(
        _serialize_ugc_room(record)
        for record in _all_ugc_records(context, public_only=True)
        if str(record["row"]["owner_player_id"]) in subscribed
    )
    skip = max(0, _BASE._int_field(dict(request.query_params), "skip", "Skip", default=0))
    take = max(0, min(100, _BASE._int_field(dict(request.query_params), "take", "Take", default=10)))
    return JSONResponse(result[skip : skip + take])


async def _handle_get_rooms_featured_v3(request: Request, context) -> Response:
    _authenticated_player(request, context)
    return JSONResponse([_serialize_coach_room(_coach_room_record(room)) for room in BUILD_COACH_ROOMS])


async def _handle_get_featured_room_group_v1(request: Request, context) -> Response:
    _authenticated_player(request, context)
    configured = context.get_server_setting("featured_room_group", None)
    if isinstance(configured, dict) and isinstance(configured.get("FeaturedRooms"), list):
        return JSONResponse(configured)
    featured = []
    for room in BUILD_COACH_ROOMS:
        serialized = _serialize_coach_room(_coach_room_record(room))
        featured.append(
            {
                "RoomName": serialized["Name"],
                "RoomId": serialized["RoomId"],
                "ImageName": serialized["ImageName"],
            }
        )
    return JSONResponse({"Name": "Rec Room Originals", "FeaturedRooms": featured})


async def _handle_get_rooms_hot_v1(request: Request, context) -> Response:
    tags = str(
        request.query_params.get("tags")
        or request.query_params.get("value")
        or ""
    ).casefold()
    _authenticated_player(request, context)
    community = _all_ugc_records(context, public_only=True)

    def room_tag_names(record: dict[str, Any]) -> set[str]:
        return {
            "community",
            # Featured is a ranked public-room feed, not a required author tag.
            "featured",
            *(
                str(value).strip().lstrip("#").casefold()
                for value in [
                    *record["metadata"].get("auto_tags", []),
                    *record["metadata"].get("custom_tags", []),
                    *record["metadata"].get("tags", []),
                ]
                if str(value).strip()
            ),
        }

    # Support space AND, pipe OR, dash NOT, and compact adjacent hashtags.
    query_groups: list[tuple[set[str], set[str]]] = []
    for raw_group in tags.split("|"):
        positives: set[str] = set()
        negatives: set[str] = set()
        for negative, value in re.findall(
            r"(-?)#?([a-z0-9][a-z0-9_-]*)", raw_group.casefold()
        ):
            normalized = "recroomoriginal" if value == "rro" else value
            (negatives if negative else positives).add(normalized)
        if positives or negatives:
            query_groups.append((positives, negatives))

    def matches_query(room_tags: set[str]) -> bool:
        # Coach tag helpers predate this evaluator and return a list. Normalize
        # both coach and UGC inputs so NOT clauses can safely use isdisjoint().
        room_tags = set(room_tags)
        if not query_groups:
            return True
        return any(
            positives.issubset(room_tags) and room_tags.isdisjoint(negatives)
            for positives, negatives in query_groups
        )

    if query_groups:
        community = [
            record for record in community
            if matches_query(room_tag_names(record))
        ]
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
    member_counts = await context.require_transient().session_member_counts(
        int(instance.get("GameSessionId") or 0) for instance in instances
    )
    active_counts: dict[int, int] = {}
    for instance in instances:
        room_id = int(instance.get("RoomId") or 0)
        session_id = str(int(instance.get("GameSessionId") or 0))
        active_counts[room_id] = active_counts.get(room_id, 0) + member_counts.get(session_id, 0)
    community.sort(
        key=_room_engagement_sort_key,
        reverse=True,
    )
    serialized = [_serialize_ugc_room(record) for record in community]
    coach_records = [_coach_room_record(room) for room in BUILD_COACH_ROOMS]
    coach = [
        _serialize_coach_room(record)
        for record in coach_records
        if matches_query(_coach_room_tag_names(record))
    ]
    if query_groups:
        return JSONResponse([*coach, *serialized])
    # Keep Coach originals as the Hot feed baseline, then rank player rooms.
    promoted = [
        room for room, record in zip(serialized, community)
        if active_counts.get(int(record["version"]["room_id"]), 0) > 0
        or int(record["metadata"].get("visit_count", 0) or 0) >= 10
        or int(record["metadata"].get("favorite_count", 0) or 0) > 0
        or int(record["metadata"].get("cheer_count", 0) or 0) > 0
    ]
    remaining = [room for room in serialized if room not in promoted]
    return JSONResponse([*promoted, *coach, *remaining])


async def _handle_get_rooms_live_v1(request: Request, context) -> Response:
    return await _handle_get_rooms_hot_v1(request, context)


async def _handle_search_rooms(request: Request, context) -> Response:
    _authenticated_player(request, context)
    value = str(
        request.query_params.get("value")
        or request.query_params.get("query")
        or request.query_params.get("searchTerm")
        or ""
    ).strip().casefold()
    # Door searches send tag expressions, not room-name queries.
    if value.startswith("#"):
        return await _handle_get_rooms_hot_v1(request, context)
    community = _all_ugc_records(context, public_only=True)
    coach_records = [_coach_room_record(room) for room in BUILD_COACH_ROOMS]
    if value:
        if value.startswith("@"):
            coach_records = []
            creator_name = value[1:].strip()
            matching_creator_ids: set[int] = set()
            with context.db.connection() as conn:
                creator_rows = conn.execute(
                    """
                    SELECT p.username, p.display_name, pvs.state_json
                    FROM players AS p
                    JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
                    WHERE pvs.api_version = ?
                    """,
                    (API_VERSION,),
                ).fetchall()
            for creator_row in creator_rows:
                try:
                    creator_state = json.loads(creator_row["state_json"] or "{}")
                except Exception:
                    creator_state = {}
                names = {
                    str(creator_row["username"] or "").casefold(),
                    str(creator_row["display_name"] or "").casefold(),
                }
                if creator_name in names:
                    matching_creator_ids.add(
                        int(creator_state.get("legacy_player_id") or 0)
                    )
            community = [
                record
                for record in community
                if int(record["creator_player_id"]) in matching_creator_ids
            ]
        else:
            compact_value = re.sub(r"\s+", "", value)
            community = [
                record for record in community
                if value in str(record["row"]["name"]).casefold()
                or value in str(record["metadata"].get("description") or "").casefold()
                or value.lstrip("#") in {
                    str(tag).strip().lstrip("#").casefold()
                    for tag in [
                        *record["metadata"].get("auto_tags", []),
                        *record["metadata"].get("custom_tags", []),
                        *record["metadata"].get("tags", []),
                    ]
                }
            ]
            coach_records = [
                record for record in coach_records
                if compact_value in re.sub(r"\s+", "", str(record["asset"]["n"]).casefold())
                or value.lstrip("#") in {"recroomoriginal", "rro"}
            ]
    elif not value:
        coach_records = list(coach_records)
    community.sort(
        key=lambda record: (
            int(record["metadata"].get("visit_count", 0) or 0) * 10
            + int(record["metadata"].get("favorite_count", 0) or 0) * 25
            + int(record["metadata"].get("cheer_count", 0) or 0) * 35
        ),
        reverse=True,
    )
    return JSONResponse([
        *(_serialize_coach_room(record) for record in coach_records),
        *(_serialize_ugc_room(record) for record in community),
    ])


async def _handle_get_ag_room_ids_v1(request: Request, context) -> Response:
    _authenticated_player(request, context)
    return JSONResponse(list(BUILD_COACH_ROOM_IDS.values()))


async def _handle_get_base_rooms(request: Request, context) -> Response:
    _authenticated_player(request, context)
    _ensure_coach_rooms(context)
    return JSONResponse([_serialize_coach_room(_coach_room_record(room)) for room in BUILD_COACH_ROOMS])


async def _handle_bookmark_room(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    if room_id <= 0:
        raise HTTPException(status_code=400, detail="RoomId is required.")
    dorm = _find_dorm_room_by_room_id(context, room_id)
    if (
        dorm is None
        and _find_coach_room_by_id(room_id, context) is None
        and _find_ugc_room(context, room_id=room_id) is None
    ):
        raise HTTPException(status_code=404, detail="Room not found.")
    bookmark = _bool_value(payload.get("Bookmark", payload.get("bookmark", False)))
    key = _canonical_player_setting_key("bookmarked_rooms", player["player_id"])
    room_ids = _BASE._get_json_setting(context, key, [])
    if not isinstance(room_ids, list):
        room_ids = []
    canonical = sorted({int(value) for value in room_ids if str(value).isdigit()})
    if bookmark and room_id not in canonical:
        canonical.append(room_id)
    if not bookmark:
        canonical = [value for value in canonical if value != room_id]
    _BASE._set_json_setting(context, key, canonical)
    _reconcile_room_social_metrics(context, room_id)
    # Do not push type 15; the bookmark callback already updates its cache.
    return JSONResponse({"Success": True, "Message": ""})


def _adjust_room_metric(context, room_id: int, metric: str, delta: int) -> None:
    coach = _find_coach_room_by_id(room_id, context)
    ugc = _find_ugc_room(context, room_id=room_id)
    canonical_room_id = coach["canonical_room_id"] if coach is not None else (
        str(ugc["row"]["room_id"]) if ugc is not None else None
    )
    if canonical_room_id is None:
        return
    with context.db.transaction() as conn:
        row = conn.execute(
            "SELECT metadata_json FROM rooms WHERE room_id = ?", (canonical_room_id,)
        ).fetchone()
        if row is None:
            return
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except Exception:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata[metric] = max(0, int(metadata.get(metric, 0) or 0) + int(delta))
        conn.execute(
            "UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE room_id = ?",
            (json.dumps(metadata, sort_keys=True), canonical_room_id),
        )


def _reconcile_room_social_metrics(context, room_id: int) -> None:
    """Rebuild room social counters from per-player membership state."""
    counts = {"favorite_count": 0, "cheer_count": 0}
    setting_metrics = {
        "bookmarked_rooms.%": "favorite_count",
        "cheered_rooms.%": "cheer_count",
    }
    with context.db.connection() as conn:
        for setting_pattern, metric in setting_metrics.items():
            rows = conn.execute(
                "SELECT value_json FROM server_settings WHERE key LIKE ?", (setting_pattern,)
            ).fetchall()
            for row in rows:
                try:
                    values = json.loads(row["value_json"] or "[]")
                except Exception:
                    values = []
                if not isinstance(values, list):
                    continue
                if room_id in {int(value) for value in values if str(value).isdigit()}:
                    counts[metric] += 1

    coach = _find_coach_room_by_id(room_id, context)
    ugc = _find_ugc_room(context, room_id=room_id)
    canonical_room_id = coach["canonical_room_id"] if coach is not None else (
        str(ugc["row"]["room_id"]) if ugc is not None else None
    )
    if canonical_room_id is None:
        return
    with context.db.transaction() as conn:
        row = conn.execute(
            "SELECT metadata_json FROM rooms WHERE room_id = ?", (canonical_room_id,)
        ).fetchone()
        if row is None:
            return
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except Exception:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(counts)
        conn.execute(
            "UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE room_id = ?",
            (json.dumps(metadata, sort_keys=True), canonical_room_id),
        )


async def _handle_cheer_room(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    if room_id <= 0:
        raise HTTPException(status_code=400, detail="RoomId is required.")
    coach = _find_coach_room_by_id(room_id, context)
    ugc = _find_ugc_room(context, room_id=room_id)
    if coach is None and ugc is None:
        raise HTTPException(status_code=404, detail="Room not found.")
    if ugc is not None and int(ugc["creator_player_id"]) == _legacy_id_for_player(player):
        return JSONResponse({"Success": False, "Message": "You cannot cheer your own room."})
    cheer = _bool_value(payload.get("Cheer", payload.get("cheer", False)))
    key = _canonical_player_setting_key("cheered_rooms", player["player_id"])
    room_ids = _BASE._get_json_setting(context, key, [])
    if not isinstance(room_ids, list):
        room_ids = []
    canonical = sorted({int(value) for value in room_ids if str(value).isdigit()})
    if cheer and room_id not in canonical:
        canonical.append(room_id)
    elif not cheer and room_id in canonical:
        canonical = [value for value in canonical if value != room_id]
    _BASE._set_json_setting(context, key, canonical)
    _reconcile_room_social_metrics(context, room_id)
    # The cheer callback owns the local one-count UI mutation. Keep the server
    # aggregate canonical, but do not push a second replacement to the sender.
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_room_filters(request: Request, context) -> Response:
    _authenticated_player(request, context)
    counts: dict[str, int] = {}
    for record in _all_ugc_records(context, public_only=True):
        for tag in [
            *record["metadata"].get("auto_tags", []),
            *record["metadata"].get("custom_tags", []),
            *record["metadata"].get("tags", []),
        ]:
            normalized = str(tag).strip().lstrip("#").casefold()
            if normalized and normalized != "community":
                counts[normalized] = counts.get(normalized, 0) + 1
    popular = [
        f"#{tag}"
        for tag, _ in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:12]
    ]
    # These are ordinary server-side discovery tags, not automatic ownership
    # or permission flags. They keep the editor usable on a fresh database.
    defaults = [
        "pvp", "hangout", "parkour", "quest", "horror", "roleplay",
        "sports", "art", "sandbox", "adventure", "social", "minigame",
    ]
    popular = list(dict.fromkeys([
        *(tag.lstrip("#") for tag in popular),
        *defaults,
    ]))[:12]
    pinned = [
        "recroomoriginal", "community", "pvp", "hangout", "parkour",
        "quest", "horror",
    ]
    return JSONResponse({
        # Return seven bare tag names; the client adds the display '#'.
        "PinnedFilters": pinned,
        "PopularFilters": popular,
    })


async def _handle_get_room_by_name_v2(name: str, request: Request, context) -> Response:
    if name.casefold() == "dormroom":
        player = _authenticated_player(request, context)
        return JSONResponse(_serialize_dorm_room(_ensure_dorm_room(player, context)))
    _authenticated_player(request, context)
    record = _find_coach_room_by_name(name, context)
    if record is not None:
        return JSONResponse(_serialize_coach_room(record))
    ugc = _find_ugc_room(context, name=name)
    if ugc is None:
        raise HTTPException(status_code=404, detail="Room not found.")
    return JSONResponse(_serialize_ugc_room(ugc))


async def _handle_get_room_details_v4(room_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    dorm = _find_dorm_room_by_room_id(context, room_id)
    if dorm is not None:
        return JSONResponse(
            _serialize_dorm_details(
                dorm, local_player_id=_legacy_id_for_player(player)
            )
        )
    _reconcile_room_social_metrics(context, room_id)
    record = _find_coach_room_by_id(room_id, context)
    if record is not None:
        return JSONResponse(_serialize_coach_details(
            record, context, local_player_id=_legacy_id_for_player(player)
        ))
    ugc = _find_ugc_room(context, room_id=room_id)
    if ugc is not None:
        return JSONResponse(
            _serialize_ugc_details(
                ugc, local_player_id=_legacy_id_for_player(player)
            )
        )
    raise HTTPException(status_code=404, detail="Room details not found.")


async def _handle_get_room_personal_details_v2(room_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    dorm = _find_dorm_room_by_room_id(context, room_id)
    if (
        dorm is None
        and _find_coach_room_by_id(room_id, context) is None
        and _find_ugc_room(context, room_id=room_id) is None
    ):
        raise HTTPException(status_code=404, detail="Room not found.")
    bookmarks = _BASE._get_json_setting(
        context, _canonical_player_setting_key("bookmarked_rooms", player["player_id"]), []
    )
    cheers = _BASE._get_json_setting(
        context, _canonical_player_setting_key("cheered_rooms", player["player_id"]), []
    )
    bookmark_ids = {int(value) for value in bookmarks if str(value).isdigit()} if isinstance(bookmarks, list) else set()
    cheer_ids = {int(value) for value in cheers if str(value).isdigit()} if isinstance(cheers, list) else set()
    return JSONResponse({
        "IsCheering": room_id in cheer_ids,
        "IsBookmarked": room_id in bookmark_ids,
    })


async def _handle_get_room_instance_details_v2(room_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    dorm = _find_dorm_room_by_room_id(context, room_id)
    if dorm is None:
        if (
            _find_coach_room_by_id(room_id, context) is None
            and _find_ugc_room(context, room_id=room_id) is None
        ):
            raise HTTPException(status_code=404, detail="Room not found.")
        with context.db.connection() as conn:
            instances = [item for item in _read_game_instances(conn) if int(item.get("RoomId") or 0) == room_id]
        member_counts = await context.require_transient().session_member_counts(
            int(instance.get("GameSessionId") or 0) for instance in instances
        )
        return JSONResponse({
            "PlayerCount": sum(member_counts.values()),
            "GameSessionCount": len(instances),
        })
    state = _player_state(player)
    active = isinstance(
        await context.require_transient().get_membership(
            _legacy_id_for_player(player)
        ),
        dict,
    )
    return JSONResponse({
        "PlayerCount": 1 if active else 0,
        "GameSessionCount": 1 if active else 0
    })


def _json_form_value(form, key: str, wrapper: str) -> list[Any] | dict[str, Any]:
    raw = form.get(key)
    if raw is None:
        return []
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="strict")
    if str(raw).strip() == "":
        return []
    try:
        payload = json.loads(str(raw))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {key} JSON.") from exc
    if isinstance(payload, dict) and wrapper in payload:
        payload = payload[wrapper]
    if not isinstance(payload, (list, dict)):
        raise HTTPException(status_code=400, detail=f"Invalid {key} payload.")
    return payload


async def _parse_multipart_fields(request: Request) -> dict[str, bytes]:
    content_type = str(request.headers.get("content-type") or "")
    if not content_type.casefold().startswith("multipart/form-data") or "boundary=" not in content_type:
        raise HTTPException(status_code=400, detail="multipart/form-data is required.")
    body = await request.body()
    message = BytesParser(policy=policy.default).parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("ascii") + body
    )
    if not message.is_multipart():
        raise HTTPException(status_code=400, detail="Invalid multipart body.")
    fields: dict[str, bytes] = {}
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if not name:
            continue
        value = part.get_payload(decode=True)
        fields[str(name)] = value if isinstance(value, bytes) else b""
    return fields


async def _handle_report_bug(request: Request, context) -> Response:
    """Persist the multipart contract emitted by RecNet.BugReporting.ReportBug."""
    player = _authenticated_player(request, context)
    fields = await _parse_multipart_fields(request)
    raw_report = fields.get("bugReport")
    if raw_report is None:
        raise HTTPException(status_code=400, detail="Multipart field bugReport is required.")
    try:
        report = json.loads(raw_report.decode("utf-8", errors="strict"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid bugReport JSON.") from exc
    if not isinstance(report, dict):
        raise HTTPException(status_code=400, detail="bugReport must be an object.")

    screenshot = fields.get("screenshotData")
    output_log = fields.get("outputLogData")
    if screenshot is not None and len(screenshot) > 8 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Screenshot exceeds 8 MiB.")
    if output_log is not None and len(output_log) > 8 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Output log exceeds 8 MiB.")

    report_id = secrets.token_hex(16)
    screenshot_name = f"{API_VERSION}-bug-{report_id}-screenshot.png" if screenshot else ""
    log_name = f"{API_VERSION}-bug-{report_id}-output.txt" if output_log else ""
    try:
        build_timestamp = int(report.get("BuildTimestamp") or 0)
    except (TypeError, ValueError):
        build_timestamp = 0
    bundle_value = report.get("BundleVersionCode")
    try:
        bundle_version_code = int(bundle_value) if bundle_value is not None else None
    except (TypeError, ValueError):
        bundle_version_code = None
    created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with context.db.transaction() as conn:
        for blob_name, data in ((screenshot_name, screenshot), (log_name, output_log)):
            if not blob_name or data is None:
                continue
            conn.execute(
                """
                INSERT INTO room_data_blobs(
                    blob_name, room_id, owner_player_id, data, image_list_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, '[]',
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                          strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                """,
                (blob_name, f"bugreport:{API_VERSION}:{report_id}", player["player_id"], data),
            )
        context.insert_bug_report(
            conn,
            report_id=report_id,
            reporter_player_id=str(player["player_id"]),
            reporter_legacy_id=_legacy_id_for_player(player),
            summary=str(report.get("Summary") or ""),
            description=str(report.get("Description") or ""),
            test_case_key=str(report.get("TestCaseKey") or ""),
            build_version=str(report.get("BuildVersion") or ""),
            build_timestamp=build_timestamp,
            bundle_version_code=bundle_version_code,
            screenshot_blob_name=screenshot_name or None,
            output_log_blob_name=log_name or None,
            source_version=API_VERSION,
            source_endpoint="api/bugreporting/v1/reportbug",
            created_at=created_at,
        )
    return Response(status_code=204)


async def _handle_upload_transient_blob(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    fields = await _parse_multipart_fields(request)
    data = fields.get("data")
    if data is None:
        raise HTTPException(status_code=400, detail="Multipart field data is required.")
    query = dict(request.query_params)
    blob_data_type = _BASE._int_field(query, "blobDataType", "BlobDataType", default=-1)
    game_session_id = _BASE._int_field(query, "gameSessionId", "GameSessionId", default=0)
    old_blob_name = _BASE._str_field(query, "oldBlobName", "OldBlobName", default="")
    active = await _repair_player_game_session(player, game_session_id, context)
    if (
        blob_data_type < 0
        or game_session_id <= 0
        or not isinstance(active, dict)
        or int(active.get("GameSessionId") or 0) != game_session_id
    ):
        raise HTTPException(status_code=409, detail="Transient blob must belong to the active game session.")
    blob_name = f"{API_VERSION}-{blob_data_type}-{secrets.token_hex(16)}.blob"
    with context.db.transaction() as conn:
        if old_blob_name:
            conn.execute(
                "DELETE FROM room_data_blobs WHERE blob_name = ? AND owner_player_id = ? AND room_id LIKE ?",
                (old_blob_name, player["player_id"], f"transient:{API_VERSION}:%"),
            )
        conn.execute(
            """
            INSERT INTO room_data_blobs(
                blob_name, room_id, owner_player_id, data, image_list_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, '[]',
                      strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                      strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            """,
            (
                blob_name,
                f"transient:{API_VERSION}:{game_session_id}:{blob_data_type}",
                player["player_id"],
                data,
            ),
        )
    # RecNet.DataBlobs.UploadTransientBlob expects BlobNameDTO.
    return JSONResponse({"BlobName": blob_name})


async def _handle_save_invention(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    _assert_invention_publishing_allowed(context, player)
    fields = await _parse_multipart_fields(request)
    data = fields.get("data")
    raw_request = fields.get("newInventionRequest")
    if data is None or raw_request is None:
        raise HTTPException(
            status_code=400, detail="Multipart fields data and newInventionRequest are required."
        )
    try:
        invention_request = json.loads(raw_request.decode("utf-8", errors="strict"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid newInventionRequest JSON.") from exc
    if not isinstance(invention_request, dict):
        raise HTTPException(status_code=400, detail="newInventionRequest must be an object.")
    name = str(invention_request.get("name", invention_request.get("Name", ""))).strip()
    description = str(
        invention_request.get("description", invention_request.get("Description", ""))
    ).strip()
    if len(name) < 3:
        return JSONResponse({"Status": 4, "Invention": None, "InventionVersion": None})
    if len(name) > 64:
        return JSONResponse({"Status": 5, "Invention": None, "InventionVersion": None})
    if len(description) > 1024:
        return JSONResponse({"Status": 10, "Invention": None, "InventionVersion": None})
    name = _filter_user_text(
        context,
        name,
        policy="censor",
        field_context="invention.name",
        player=player,
    )
    description = _filter_user_text(
        context,
        description,
        policy="censor",
        field_context="invention.description",
        player=player,
    )
    player_id = _legacy_id_for_player(player)
    records = _BASE._get_json_setting(context, INVENTIONS_SETTING, [])
    if not isinstance(records, list):
        records = []
    if any(
        isinstance(item, dict)
        and isinstance(item.get("Invention"), dict)
        and int(item["Invention"].get("CreatorPlayerId") or 0) == player_id
        and str(item["Invention"].get("Name") or "").casefold() == name.casefold()
        for item in records
    ):
        return JSONResponse({"Status": 3, "Invention": None, "InventionVersion": None})
    creation_room_id = _BASE._int_field(
        invention_request, "creationRoomId", "CreationRoomId", default=0
    )
    active = await context.require_transient().get_membership(player_id)
    if not isinstance(active, dict):
        active = _player_state(player).get("game_session")
    if creation_room_id > 0 and (
        not isinstance(active, dict) or int(active.get("RoomId") or 0) != creation_room_id
    ):
        return JSONResponse({"Status": 21, "Invention": None, "InventionVersion": None})
    invention_id = max(
        (
            int(item.get("Invention", {}).get("InventionId") or 0)
            for item in records
            if isinstance(item, dict) and isinstance(item.get("Invention"), dict)
        ),
        default=0,
    ) + 1
    blob_name = f"{API_VERSION}-invention-{invention_id}-{secrets.token_hex(8)}.blob"
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    invention = {
        "InventionId": invention_id,
        "CreatorPlayerId": player_id,
        "Name": name,
        "Description": description,
        "ImageName": str(invention_request.get("imageName") or invention_request.get("ImageName") or ""),
        "CurrentVersionNumber": 1,
        "IsPublished": False,
        "ModifiedAt": now,
        "CreatedAt": now,
        "FirstPublishedAt": None,
        "CreationRoomId": creation_room_id if creation_room_id > 0 else None,
        "NumPlayersHaveUsedInRoom": 0,
        "NumDownloads": 0,
        "CheerCount": 0,
        "CreatorPermission": 100,
        "GeneralPermission": 0,
    }
    version = {
        "InventionId": invention_id,
        "VersionNumber": 1,
        "InstantiationCost": max(
            0, _BASE._int_field(invention_request, "instantiationCost", "InstantiationCost", default=0)
        ),
        "LightsCost": max(
            0, _BASE._int_field(invention_request, "lightsCost", "LightsCost", default=0)
        ),
        "BlobName": blob_name,
    }
    referenced_images = invention_request.get("referencedImages", [])
    if not isinstance(referenced_images, list):
        referenced_images = []
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO room_data_blobs(
                blob_name, room_id, owner_player_id, data, image_list_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?,
                      strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                      strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            """,
            (
                blob_name,
                f"invention:{API_VERSION}:{invention_id}",
                player["player_id"],
                data,
                json.dumps(referenced_images),
            ),
        )
    records.append(
        {
            "Invention": invention,
            "InventionVersion": version,
            "Versions": [version],
            "AutoTags": [
                str(tag).strip().lstrip("#")
                for tag in invention_request.get("autoTags", invention_request.get("AutoTags", []))
                if str(tag).strip()
            ] if isinstance(
                invention_request.get("autoTags", invention_request.get("AutoTags", [])),
                list,
            ) else [],
            "PlayerAddedTags": [],
            "ReferencedImages": referenced_images,
            "ReferencedData": invention_request.get("referencedData", []),
            "ReferencedInventions": invention_request.get("referencedInventions", []),
        }
    )
    _BASE._set_json_setting(context, INVENTIONS_SETTING, records[-500:])
    return JSONResponse({"Status": 0, "Invention": invention, "InventionVersion": version})


def _image_asset_row(
    context,
    image_name: str,
    *,
    owner_player_id: str | None = None,
):
    if not re.fullmatch(r"[0-9a-f-]{36}\.(?:jpg|jpeg|png)", image_name, flags=re.IGNORECASE):
        return None
    query = """
        SELECT * FROM data_assets
        WHERE (relative_path = ? OR relative_path = ? OR relative_path LIKE ?)
    """
    params: list[Any] = [f"IMAGES/{image_name}", image_name, f"%/{image_name}"]
    if owner_player_id is not None:
        query += " AND owner_player_id = ?"
        params.append(owner_player_id)
    query += " LIMIT 1"
    with context.db.connection() as conn:
        row = conn.execute(query, tuple(params)).fetchone()
    if row is None or not context.image_asset_is_available(str(row["asset_id"])):
        return None
    return row


async def _handle_list_saved_images(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT da.relative_path
            FROM data_assets AS da
            WHERE owner_player_id = ? AND purpose = ?
              AND NOT EXISTS (
                  SELECT 1 FROM image_moderation_jobs AS imj
                  WHERE imj.asset_id = da.asset_id AND imj.decision != 'safe'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM moderation_content_controls AS mcc
                  WHERE mcc.target_type = 'image'
                    AND mcc.target_id = da.asset_id
                    AND mcc.control_type = 'quarantine'
                    AND mcc.active = 1
              )
            ORDER BY created_at DESC
            """,
            (player["player_id"], f"{API_VERSION}.saved_image"),
        ).fetchall()
    return JSONResponse(
        {"Images": [Path(str(row["relative_path"])).name for row in rows]},
        headers={"Cache-Control": "no-store"},
    )


def _saved_image_numeric_id(asset_id: str) -> int:
    compact = re.sub(r"[^0-9a-f]", "", str(asset_id).casefold())
    if len(compact) < 15:
        compact = (compact + ("0" * 15))[:15]
    # Fifteen hexadecimal digits stay safely inside signed Int64 while
    # remaining stable across server restarts and Railway replicas.
    return int(compact[:15], 16)


async def _saved_image_upload_metadata(
    raw_metadata: dict[str, Any], player, context
) -> dict[str, Any]:
    """Normalize SavedImageMetaDTO without trusting author-like client data."""
    tagged_player_ids: list[int] = []
    raw_player_ids = raw_metadata.get("playerIds", [])
    if isinstance(raw_player_ids, list):
        for value in raw_player_ids:
            if not str(value).lstrip("-").isdigit():
                continue
            player_id = int(value)
            if (
                player_id > 0
                and player_id not in tagged_player_ids
                and _find_player_by_legacy_id_25april2019(context, player_id) is not None
            ):
                tagged_player_ids.append(player_id)
    saved_image_type = _BASE._int_field(
        raw_metadata, "savedImageType", "SavedImageType", default=0
    )
    accessibility = _BASE._int_field(
        raw_metadata, "accessibility", "Accessibility", default=0
    )
    submitted_room_id = _BASE._int_field(raw_metadata, "roomId", "RoomId", default=-1)
    submitted_player_event_id = _BASE._int_field(
        raw_metadata, "playerEventId", "PlayerEventId", default=-1
    )
    if saved_image_type not in {0, 1, 2, 3, 4, 5}:
        raise HTTPException(status_code=400, detail="Invalid savedImageType.")
    if accessibility not in {0, 1, 2}:
        raise HTTPException(status_code=400, detail="Invalid saved image accessibility.")

    author_id = _legacy_id_for_player(player)
    state = _player_state(player)
    active_session = await _authoritative_game_session_for_player(author_id, state, context)
    if active_session is None:
        room_id = submitted_room_id
        player_event_id = submitted_player_event_id
        game_session_id = 0
        room_scene_id = 0
        photon_room_id = ""
    else:
        # Bind captures to the authenticated player's current membership.
        room_id = int(active_session.get("RoomId") or submitted_room_id)
        player_event_id = int(
            active_session.get("PlayerEventId")
            if active_session.get("PlayerEventId") is not None
            else submitted_player_event_id
        )
        game_session_id = int(active_session.get("GameSessionId") or 0)
        room_scene_id = int(active_session.get("RoomSceneId") or 0)
        photon_room_id = str(active_session.get("PhotonRoomId") or "")
        # Tagged player IDs are subjects; the authenticated uploader is the author.
    return {
        "playerIds": tagged_player_ids,
        "taggedPlayerIds": tagged_player_ids,
        "savedImageType": saved_image_type,
        "roomId": room_id,
        "playerEventId": player_event_id,
        "gameSessionId": game_session_id,
        "roomSceneId": room_scene_id,
        "photonRoomId": photon_room_id,
        "accessibility": accessibility,
        "authorPlayerId": author_id,
        "authorUsername": str(player["username"] or player["display_name"] or f"Player{author_id}"),
        "capturedAt": _format_recnet_datetime(datetime.now(timezone.utc)),
    }


def _public_slideshow_asset_rows(context) -> list[Any]:
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT
                da.*,
                p.username AS owner_username,
                p.display_name AS owner_display_name,
                pvs.state_json AS owner_state_json
            FROM data_assets AS da
            JOIN players AS p ON p.player_id = da.owner_player_id
            LEFT JOIN player_version_state AS pvs
              ON pvs.player_id = p.player_id AND pvs.api_version = ?
            WHERE da.purpose = ?
              AND NOT EXISTS (
                  SELECT 1 FROM image_moderation_jobs AS imj
                  WHERE imj.asset_id = da.asset_id AND imj.decision != 'safe'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM moderation_content_controls AS mcc
                  WHERE mcc.target_type = 'image'
                    AND mcc.target_id = da.asset_id
                    AND mcc.control_type = 'quarantine'
                    AND mcc.active = 1
              )
            ORDER BY da.created_at DESC
            LIMIT 100
            """,
            (API_VERSION, f"{API_VERSION}.saved_image"),
        ).fetchall()
    public_rows = []
    for row in rows:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except Exception:
            metadata = {}
        if not isinstance(metadata, dict):
            continue
        try:
            accessibility = int(metadata.get("accessibility", 0) or 0)
            saved_image_type = int(metadata.get("savedImageType", 0) or 0)
        except (TypeError, ValueError):
            continue
        # The Rec Center board accepts only public Share Camera photos.
        if accessibility == 1 and saved_image_type == 1:
            public_rows.append((row, metadata))
    return public_rows


def _slideshow_room_identity(context, metadata: dict[str, Any]) -> tuple[str, int | None]:
    try:
        room_id = int(metadata.get("roomId", 0) or 0)
    except (TypeError, ValueError):
        room_id = 0
    if room_id <= 0:
        return "", None
    coach = _find_coach_room_by_id(room_id, context)
    if coach is not None:
        return str(coach["asset"]["n"]), room_id
    ugc = _find_ugc_room(context, room_id=room_id)
    if ugc is not None:
        return str(ugc["row"]["name"]), room_id
    with context.db.connection() as conn:
        dorm_rows = conn.execute(
            "SELECT metadata_json FROM rooms WHERE lower(name) = 'dormroom'"
        ).fetchall()
    for row in dorm_rows:
        try:
            version = (
                json.loads(row["metadata_json"] or "{}")
                .get("versions", {})
                .get(API_VERSION, {})
            )
        except Exception:
            version = {}
        if isinstance(version, dict) and int(version.get("room_id") or 0) == room_id:
            return "DormRoom", room_id
    return "", room_id


async def _handle_current_slideshow(request: Request, context) -> Response:
    _authenticated_player(request, context)
    images = []
    for row, metadata in _public_slideshow_asset_rows(context):
        try:
            owner_state = json.loads(row["owner_state_json"] or "{}")
        except Exception:
            owner_state = {}
        if not isinstance(owner_state, dict):
            owner_state = {}
        # Attribute photos to owner_player_id, never to tagged subjects.
        player_id = int(
            owner_state.get("legacy_player_id")
            or owner_state.get("recnet_id")
            or 0
        )
        if player_id <= 0:
            continue
        room_name, room_id = _slideshow_room_identity(context, metadata)
        images.append({
            "SavedImageId": _saved_image_numeric_id(str(row["asset_id"])),
            "ImageName": Path(str(row["relative_path"])).name,
            "Username": str(
                row["owner_username"]
                or row["owner_display_name"]
                or f"Player{player_id}"
            ),
            "PlayerId": player_id,
            "RoomName": room_name,
            "RoomId": room_id,
        })
    if not images:
        # RecentImageProjector requires a nonempty array; fallback data is response-only.
        images.append({
            "SavedImageId": 0,
            "ImageName": DEFAULT_IMAGE_NAME,
            "Username": "DOESNOTEXIST",
            "PlayerId": 0,
            "RoomName": "RecCenter",
            "RoomId": BUILD_COACH_ROOM_IDS["reccenter"],
        })
    # RecentImageProjector refreshes when ValidTill expires. A short window
    # makes new public photos appear without requiring a room or game restart.
    valid_till = datetime.now(timezone.utc) + timedelta(seconds=30)
    return JSONResponse(
        {
            "ValidTill": valid_till.isoformat().replace("+00:00", "Z"),
            "Images": images,
        },
        headers={"Cache-Control": "no-store"},
    )


async def _handle_modify_saved_image_accessibility(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    image_name = _BASE._str_field(payload, "ImageName", "imageName").strip()
    accessibility = _BASE._int_field(
        payload,
        "Accessibility",
        "accessibility",
        default=-1,
    )
    if accessibility not in {0, 1, 2}:
        raise HTTPException(status_code=400, detail="Invalid saved image accessibility.")
    row = _image_asset_row(
        context,
        image_name,
        owner_player_id=str(player["player_id"]),
    )
    if row is None or str(row["purpose"]) != f"{API_VERSION}.saved_image":
        raise HTTPException(status_code=404, detail="Saved image not found.")
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except Exception:
        metadata = {}
    if not isinstance(metadata, dict):
        metadata = {}
    metadata["accessibility"] = accessibility
    with context.db.transaction() as conn:
        conn.execute(
            "UPDATE data_assets SET metadata_json = ? WHERE asset_id = ?",
            (json.dumps(metadata, sort_keys=True), str(row["asset_id"])),
        )
    return Response(status_code=204)


async def _handle_delete_image(
    request: Request,
    context,
    *,
    expected_purpose: str,
) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    image_name = _BASE._str_field(payload, "ImageName", "imageName").strip()
    row = _image_asset_row(
        context,
        image_name,
        owner_player_id=str(player["player_id"]),
    )
    if row is None or str(row["purpose"]) != expected_purpose:
        # Image cleanup is idempotent.
        return Response(status_code=204)
    path = (context.data_dir / str(row["relative_path"])).resolve()
    with context.db.transaction() as conn:
        conn.execute(
            "DELETE FROM data_assets WHERE asset_id = ?",
            (str(row["asset_id"]),),
        )
    if context.data_dir.resolve() in path.parents:
        path.unlink(missing_ok=True)
    return Response(status_code=204)


async def _handle_saved_image_link(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    image_name = _BASE._str_field(payload, "ImageName", "imageName").strip()
    row = _image_asset_row(
        context,
        image_name,
        owner_player_id=str(player["player_id"]),
    )
    if row is None or str(row["purpose"]) != f"{API_VERSION}.saved_image":
        raise HTTPException(status_code=404, detail="Saved image not found.")
    # This private restoration has no external SMS/email link service. The
    # endpoint is an acknowledged share action and does not retain contact data.
    return Response(status_code=204)


async def _handle_cheer_image(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    image_id = _BASE._int_field(
        payload, "SavedImageId", "savedImageId", "ImageId", "imageId", default=0
    )
    if image_id <= 0:
        raise HTTPException(status_code=400, detail="ImageId is required.")
    matching = next(
        (
            (row, metadata)
            for row, metadata in _public_slideshow_asset_rows(context)
            if _saved_image_numeric_id(str(row["asset_id"])) == image_id
        ),
        None,
    )
    if matching is None:
        raise HTTPException(status_code=404, detail="Slideshow image not found.")
    cheer = bool(payload.get("Cheer", payload.get("cheer", False)))
    key = _canonical_player_setting_key("cheered_images", player["player_id"])
    existing = _BASE._get_json_setting(context, key, [])
    if not isinstance(existing, list):
        existing = []
    image_ids = sorted({int(value) for value in existing if str(value).isdigit()})
    was_cheering = image_id in image_ids
    if cheer and not was_cheering:
        image_ids.append(image_id)
    elif not cheer and was_cheering:
        image_ids = [value for value in image_ids if value != image_id]
    _BASE._set_json_setting(context, key, sorted(image_ids))

    row, metadata = matching
    metadata["cheerCount"] = max(
        0,
        int(metadata.get("cheerCount", 0) or 0)
        + (1 if cheer and not was_cheering else -1 if not cheer and was_cheering else 0),
    )
    with context.db.transaction() as conn:
        conn.execute(
            "UPDATE data_assets SET metadata_json = ? WHERE asset_id = ?",
            (json.dumps(metadata, sort_keys=True), str(row["asset_id"])),
        )
    return Response(status_code=204)


async def _save_2019_moderated_image(
    context,
    *,
    owner_player_id: str,
    content: bytes,
    file_ext: str,
    mime_type: str,
    purpose: str,
    metadata: dict[str, Any],
    activation_type: str = "publish",
    activation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manager = context.image_moderation
    if manager is None:
        raise HTTPException(status_code=503, detail="Image moderation is unavailable.")
    try:
        return await asyncio.to_thread(
            context.save_image_bytes,
            owner_player_id=owner_player_id,
            content=content,
            file_ext=file_ext,
            mime_type=mime_type,
            purpose=purpose,
            metadata=metadata,
            target_size=manager.target_size,
            moderation={
                "activation_type": activation_type,
                "activation": activation or {},
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


async def handle_image_moderation_approved(*, job: dict[str, Any], context) -> None:
    if str(job.get("activation_type") or "") != "profile":
        return
    owner_player_id = str(job["owner_player_id"])
    with context.db.transaction() as conn:
        latest = conn.execute(
            """
            SELECT job_id
            FROM image_moderation_jobs
            WHERE owner_player_id = ? AND activation_type = 'profile'
            ORDER BY created_at DESC, rowid DESC
            LIMIT 1
            """,
            (owner_player_id,),
        ).fetchone()
        if latest is None or str(latest["job_id"]) != str(job["job_id"]):
            return
        asset = conn.execute(
            "SELECT relative_path FROM data_assets WHERE asset_id = ?",
            (str(job["asset_id"]),),
        ).fetchone()
        player_state = conn.execute(
            """
            SELECT state_json
            FROM player_version_state
            WHERE player_id = ? AND api_version = ?
            """,
            (owner_player_id, API_VERSION),
        ).fetchone()
        if asset is None or player_state is None:
            raise RuntimeError("Approved profile image owner state is missing.")
        try:
            state = json.loads(player_state["state_json"] or "{}")
        except (TypeError, ValueError):
            state = {}
        if not isinstance(state, dict):
            state = {}
        state["profile_image_name"] = Path(str(asset["relative_path"])).name
        activation = job.get("activation")
        if not isinstance(activation, dict):
            activation = {}
        legacy_player_id = int(
            activation.get("legacy_player_id")
            or state.get("legacy_player_id")
            or state.get("recnet_id")
            or 0
        )
        conn.execute(
            """
            UPDATE players
            SET profile_picture_asset_id = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ?
            """,
            (str(job["asset_id"]), owner_player_id),
        )
        conn.execute(
            """
            UPDATE player_version_state
            SET state_json = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE player_id = ? AND api_version = ?
            """,
            (json.dumps(state, sort_keys=True), owner_player_id, API_VERSION),
        )
    if legacy_player_id > 0:
        await _broadcast_profile_update(legacy_player_id, context)


async def _handle_upload_transient_image(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    active = await context.require_transient().get_membership(
        _legacy_id_for_player(player)
    )
    requested_session_id = _BASE._int_field(
        request.query_params,
        "gameSessionId",
        "GameSessionId",
        default=0,
    )
    if (
        not isinstance(active, dict)
        or requested_session_id <= 0
        or int(active.get("GameSessionId") or 0) != requested_session_id
    ):
        raise HTTPException(status_code=409, detail="Not in the requested game session.")
    fields = await _parse_multipart_fields(request)
    image = fields.get("image")
    if not image:
        raise HTTPException(status_code=400, detail="An image field is required.")
    is_jpeg = image.startswith(b"\xff\xd8")
    is_png = image.startswith(b"\x89PNG\r\n\x1a\n")
    if not is_jpeg and not is_png:
        raise HTTPException(status_code=400, detail="A PNG or JPEG image is required.")
    file_ext = ".jpg" if is_jpeg else ".png"
    mime_type = "image/jpeg" if is_jpeg else "image/png"
    old_image_name = str(
        request.query_params.get("oldImageName")
        or request.query_params.get("OldImageName")
        or ""
    ).strip()
    asset = await _save_2019_moderated_image(
        context,
        owner_player_id=str(player["player_id"]),
        content=image,
        file_ext=file_ext,
        mime_type=mime_type,
        purpose=f"{API_VERSION}.transient_image",
        metadata={
            "api_version": API_VERSION,
            "gameSessionId": requested_session_id,
            "oldImageName": old_image_name,
        },
    )
    return JSONResponse({"ImageName": Path(asset["relative_path"]).name})


async def _handle_upload_saved_image(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    fields = await _parse_multipart_fields(request)
    image = fields.get("image")
    if not image or not image.startswith(b"\xff\xd8"):
        raise HTTPException(status_code=400, detail="A JPEG image field is required.")
    raw_metadata_dict: dict[str, Any] = {}
    raw_metadata = fields.get("imgMeta")
    if raw_metadata:
        try:
            parsed = json.loads(raw_metadata.decode("utf-8"))
            if isinstance(parsed, dict):
                raw_metadata_dict = parsed
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid imgMeta JSON.") from exc
    metadata = await _saved_image_upload_metadata(raw_metadata_dict, player, context)
    asset = await _save_2019_moderated_image(
        context,
        owner_player_id=str(player["player_id"]),
        content=image,
        file_ext=".jpg",
        mime_type="image/jpeg",
        purpose=f"{API_VERSION}.saved_image",
        metadata=metadata,
    )
    return JSONResponse({"ImageName": Path(asset["relative_path"]).name})


async def _handle_set_profile_image(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    fields = await _parse_multipart_fields(request)
    image = fields.get("image")
    if not image or not image.startswith(b"\xff\xd8"):
        raise HTTPException(status_code=400, detail="A JPEG image field is required.")
    state = _player_state(player)
    legacy_player_id = int(state.get("legacy_player_id") or state.get("recnet_id") or 0)
    await _save_2019_moderated_image(
        context,
        owner_player_id=str(player["player_id"]),
        content=image,
        file_ext=".jpg",
        mime_type="image/jpeg",
        purpose=f"{API_VERSION}.profile_image",
        metadata={"api_version": API_VERSION},
        activation_type="profile",
        activation={"legacy_player_id": legacy_player_id},
    )
    return Response(status_code=204)


async def _handle_uploaded_image(image_name: str, request: Request, context) -> Response:
    published_name = next(
        (name for name in set(COACH_ROOM_IMAGE_TEXTURES.values()) if name.casefold() == image_name.casefold()),
        None,
    )
    if published_name is not None:
        resp = context.serve_image(published_name)
        if resp is not None:
            return resp
        published_path = (ROOM_IMAGE_DATA_DIR / published_name).resolve()
        if published_path.is_file() and ROOM_IMAGE_DATA_DIR.resolve() in published_path.parents:
            return Response(
                content=published_path.read_bytes(),
                media_type="image/png",
                headers={
                    "Cache-Control": "public, max-age=31536000, immutable",
                    "Last-Modified": DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
                },
            )
        raise HTTPException(status_code=404, detail="Room image was not published with the server.")
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT asset_id, relative_path, mime_type FROM data_assets
            WHERE relative_path = ? OR relative_path = ? OR relative_path LIKE ? LIMIT 1
            """,
            (f"IMAGES/{image_name}", image_name, f"%/{image_name}"),
        ).fetchone()
    if row is not None and context.image_asset_is_available(str(row["asset_id"])):
        path = (context.data_dir / str(row["relative_path"])).resolve()
        if path.is_file() and context.data_dir.resolve() in path.parents:
            return Response(
                content=path.read_bytes(),
                media_type=str(row["mime_type"]),
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )
    resp = context.serve_image(image_name)
    if resp is not None:
        return resp
    raise HTTPException(status_code=404, detail="Image not found.")


def _coach_record_and_scene_by_scene_id(scene_id: int, context):
    for room in BUILD_COACH_ROOMS:
        record = _coach_room_record(room)
        for scene in room["x"]:
            key = (str(room["n"]).casefold(), str(scene["n"]).casefold())
            if BUILD_COACH_SCENE_IDS[key] == scene_id:
                return record, scene
    return None, None


def _owned_room_scene_record(scene_id: int, player, context) -> tuple[str, str, dict[str, Any]] | None:
    """Return (kind, canonical blob-room key, record) for an owned mutable scene."""
    dorm = _ensure_dorm_room(player, context)
    if int(dorm["version"]["scene_id"]) == scene_id:
        return "dorm", f"{dorm['row']['room_id']}:{scene_id}", dorm
    ugc = _find_ugc_room_by_scene_id(context, scene_id)
    if ugc is not None and _player_can_edit_ugc(ugc, player):
        return "ugc", f"{ugc['row']['room_id']}:{scene_id}", ugc
    return None


async def _handle_room_data_history(scene_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    owned = _owned_room_scene_record(scene_id, player, context)
    if owned is None:
        # The client expects a bare List<RoomDataHistoryDTO>. Do not expose
        # another player's immutable blob names through this owner-only UI.
        return JSONResponse([])
    _kind, canonical_scene_id, _record = owned
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT rowid AS history_id, blob_name, created_at
            FROM room_data_blobs
            WHERE room_id = ?
            ORDER BY created_at DESC, rowid DESC
            """,
            (canonical_scene_id,),
        ).fetchall()
    return JSONResponse(
        [
            {
                "RoomDataHistoryId": int(row["history_id"]),
                "DataBlobName": str(row["blob_name"]),
                "CreatedAt": str(row["created_at"]),
            }
            for row in rows
        ]
    )


async def _handle_restore_room_data_history(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    history_id = _BASE._int_field(
        payload, "RoomDataHistoryId", "roomDataHistoryId", default=0
    )
    with context.db.connection() as conn:
        history = conn.execute(
            """
            SELECT rowid AS history_id, blob_name, room_id
            FROM room_data_blobs WHERE rowid = ?
            """,
            (history_id,),
        ).fetchone()
    if history is None:
        return JSONResponse({"Result": 4, "RoomScene": None})
    try:
        scene_id = int(str(history["room_id"]).rsplit(":", 1)[1])
    except (IndexError, TypeError, ValueError):
        return JSONResponse({"Result": 4, "RoomScene": None})
    owned = _owned_room_scene_record(scene_id, player, context)
    if owned is None or str(history["room_id"]) != owned[1]:
        return JSONResponse({"Result": 2, "RoomScene": None})

    kind, _canonical_scene_id, record = owned
    blob_name = str(history["blob_name"])
    record["version"]["data_blob_name"] = blob_name
    with context.db.transaction() as conn:
        conn.execute(
            """
            UPDATE rooms
            SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE room_id = ?
            """,
            (json.dumps(record["metadata"], sort_keys=True), record["row"]["room_id"]),
        )
        instances = _read_game_instances(conn)
        changed = False
        for instance in instances:
            if int(instance.get("RoomSceneId") or 0) == scene_id:
                instance["DataBlobName"] = blob_name
                changed = True
        if changed:
            _write_game_instances(conn, instances)

    active = await context.require_transient().get_membership(
        _legacy_id_for_player(player)
    )
    if isinstance(active, dict) and int(active.get("RoomSceneId") or 0) == scene_id:
        active["DataBlobName"] = blob_name
        await _persist_active_game_session(player, active, context)
    if kind == "dorm":
        refreshed = _ensure_dorm_room(player, context)
        scene = _serialize_dorm_scene(refreshed)
    else:
        refreshed = _find_ugc_room_by_scene_id(context, scene_id)
        scene = _serialize_ugc_scene(refreshed) if refreshed is not None else None
    return JSONResponse({"Result": 0 if scene is not None else 4, "RoomScene": scene})


async def _handle_save_room_data(scene_id: int, request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    active = await context.require_transient().get_membership(
        _legacy_id_for_player(player)
    )
    if not isinstance(active, dict) or int(active.get("RoomSceneId") or 0) != scene_id:
        raise HTTPException(status_code=403, detail="Only the active room scene can be saved.")

    # Coach source scenes are immutable despite their temporary Maker Pen role.
    record, scene = _coach_record_and_scene_by_scene_id(scene_id, context)
    if record is not None and scene is not None:
        raise HTTPException(status_code=404, detail="Room scene not found.")

    form = await _parse_multipart_fields(request)
    upload = form.get("data")
    if upload is None:
        raise HTTPException(status_code=400, detail="Multipart field 'data' is required.")
    data = bytes(upload)
    if not data:
        raise HTTPException(status_code=400, detail="Room data cannot be empty.")
    image_list = _json_form_value(form, "imgList", "roomImageList")
    data_blob_list = _json_form_value(form, "dataBlobList", "dataBlobList")
    invention_usages = _json_form_value(form, "inventionUsages", "roomInventionUsages")

    dorm = _ensure_dorm_room(player, context)
    ugc = _find_ugc_room_by_scene_id(context, scene_id)
    local_id = _legacy_id_for_player(player)
    if scene_id == int(dorm["version"]["scene_id"]):
        canonical_scene_id = f"{dorm['row']['room_id']}:{scene_id}"
        is_dorm = True
        is_ugc = False
    elif ugc is not None:
        if not _player_can_edit_ugc(ugc, player):
            raise HTTPException(status_code=403, detail="Only the room creator or a co-owner can save this room.")
        canonical_scene_id = f"{ugc['row']['room_id']}:{scene_id}"
        is_dorm = False
        is_ugc = True
    else:
        raise HTTPException(status_code=404, detail="Room scene not found.")

    blob_name = f"{API_VERSION}-{scene_id}-{secrets.token_hex(12)}.room"
    metadata = {
        "roomImageList": image_list,
        "dataBlobList": data_blob_list,
        "roomInventionUsages": invention_usages,
        "savedByPlayerId": local_id,
    }
    with context.db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO room_data_blobs(
                blob_name, room_id, owner_player_id, data, image_list_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                      strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            """,
            (blob_name, canonical_scene_id, player["player_id"], data, json.dumps(metadata, sort_keys=True)),
        )
        if is_dorm:
            room_metadata = dorm["metadata"]
            room_metadata["versions"][API_VERSION]["data_blob_name"] = blob_name
            conn.execute(
                """
                UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE room_id = ?
                """,
                (json.dumps(room_metadata, sort_keys=True), dorm["row"]["room_id"]),
            )
        elif is_ugc:
            room_metadata = ugc["metadata"]
            room_metadata["versions"][API_VERSION]["data_blob_name"] = blob_name
            conn.execute(
                """
                UPDATE rooms SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE room_id = ?
                """,
                (json.dumps(room_metadata, sort_keys=True), ugc["row"]["room_id"]),
            )
        instances = _read_game_instances(conn)
        instance_changed = False
        for instance in instances:
            if (
                int(instance.get("GameSessionId") or 0)
                == int(active.get("GameSessionId") or 0)
                and int(instance.get("RoomSceneId") or 0) == scene_id
            ):
                instance["DataBlobName"] = blob_name
                instance_changed = True
        if instance_changed:
            _write_game_instances(conn, instances)

    # Update the live session metadata to the newly saved room blob.
    active["DataBlobName"] = blob_name
    await _persist_active_game_session(player, active, context)

    if is_dorm:
        refreshed = _ensure_dorm_room(player, context)
        await _send_hub_notification(
            local_id,
            15,
            _serialize_dorm_details(refreshed, local_player_id=local_id),
            context=context,
        )
        return JSONResponse(_serialize_dorm_scene(refreshed))
    if is_ugc:
        refreshed = _find_ugc_room(context, room_id=int(ugc["version"]["room_id"]))
        if refreshed is None:
            raise HTTPException(status_code=404, detail="Saved room could not be reloaded.")
        await _send_hub_notification(
            local_id,
            15,
            _serialize_ugc_details(refreshed, local_player_id=local_id),
            context=context,
        )
        return JSONResponse(_serialize_ugc_scene(refreshed))
    raise HTTPException(status_code=404, detail="Room scene not found.")


async def _handle_get_room_data(blob_name: str, request: Request, context) -> Response:
    # Room blobs use unguessable names because CDN-style fetches are unauthenticated.
    if not re.fullmatch(r"[A-Za-z0-9._-]+", blob_name):
        raise HTTPException(status_code=404, detail="Room data not found.")
    with context.db.connection() as conn:
        row = conn.execute(
            "SELECT data, updated_at FROM room_data_blobs WHERE blob_name = ?", (blob_name,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Room data not found.")
    return Response(
        content=bytes(row["data"]), media_type="application/octet-stream",
        headers={"Last-Modified": str(row["updated_at"]), "Cache-Control": "no-cache"},
    )


async def _handle_invention_creator_ids(request: Request, context) -> Response:
    _authenticated_player(request, context)
    raw_room_id = request.query_params.get("roomId") or request.query_params.get("RoomId")
    try:
        room_id = int(raw_room_id or 0)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="roomId must be numeric.") from exc
    if room_id <= 0:
        raise HTTPException(status_code=400, detail="roomId is required.")
    coach = _find_coach_room_by_id(room_id, context)
    ugc = _find_ugc_room(context, room_id=room_id) if coach is None else None
    dorm = _find_dorm_room_by_room_id(context, room_id) if coach is None and ugc is None else None
    if coach is not None:
        canonical_room_id = str(coach["canonical_room_id"])
    elif ugc is not None:
        canonical_room_id = str(ugc["row"]["room_id"])
    elif dorm is not None:
        canonical_room_id = str(dorm["row"]["room_id"])
    else:
        raise HTTPException(status_code=404, detail="Room not found.")

    invention_ids: set[int] = set()
    with context.db.connection() as conn:
        rows = conn.execute(
            """
            SELECT image_list_json FROM room_data_blobs
            WHERE room_id LIKE ?
            ORDER BY created_at DESC, rowid DESC
            """,
            (f"{canonical_room_id}:%",),
        ).fetchall()
    for row in rows:
        try:
            metadata = json.loads(row["image_list_json"] or "{}")
        except Exception:
            continue
        usages = metadata.get("roomInventionUsages", []) if isinstance(metadata, dict) else []
        if isinstance(usages, dict):
            candidates = usages.keys()
        elif isinstance(usages, list):
            candidates = [
                item.get("InventionId", item.get("inventionId", 0))
                if isinstance(item, dict) else item
                for item in usages
            ]
        else:
            candidates = []
        invention_ids.update(
            int(value) for value in candidates
            if str(value).lstrip("-").isdigit() and int(value) > 0
        )
    creator_ids = sorted(
        {
            int(record["Invention"].get("CreatorPlayerId") or 0)
            for invention_id in invention_ids
            for record in [_find_invention_record(context, invention_id)]
            if isinstance(record, dict)
            and isinstance(record.get("Invention"), dict)
            and int(record["Invention"].get("CreatorPlayerId") or 0) > 0
        }
    )
    # Return the exact InventionCreatorIdDTO field name.
    return JSONResponse([{"CreatorPlayerId": creator_id} for creator_id in creator_ids])


async def _handle_set_game_in_progress(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    game_session_id = _BASE._int_field(payload, "GameSessionId", "gameSessionId", default=0)
    in_progress = bool(payload.get("InProgress", payload.get("inProgress", False)))
    active = await _repair_player_game_session(player, game_session_id, context)
    if not isinstance(active, dict) or int(active.get("GameSessionId") or 0) != game_session_id:
        raise HTTPException(status_code=409, detail="Game session is not active for this player.")
    with context.db.transaction() as conn:
        instances = _read_game_instances(conn)
        instance = next((item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id), None)
        if instance is None:
            raise HTTPException(status_code=404, detail="Game session not found.")
        instance["GameInProgress"] = in_progress
        _write_game_instances(conn, instances)
    active["GameInProgress"] = in_progress
    await _persist_active_game_session(player, active, context)
    return Response(status_code=204)


async def _handle_send_message(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    target_id = _BASE._int_field(payload, "ToPlayerId", "toPlayerId", default=0)
    message_type = _BASE._int_field(payload, "Type", "type", default=-1)
    source_id = _legacy_id_for_player(player)
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target_id <= 0 or target is None:
        raise HTTPException(status_code=404, detail="Target player not found.")
    if target_id == source_id:
        raise HTTPException(status_code=400, detail="A player cannot message themselves.")
    if message_type not in {0, 1, 2, 3, 5, 10, 11, 20, 30}:
        raise HTTPException(status_code=400, detail="Unsupported message type.")
    if _players_ignore_each_other(context, source_id, target_id):
        raise HTTPException(status_code=403, detail="The players cannot message each other.")
    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    if message_type == 0:
        active = await _active_game_session_for_player(player, context)
        if not isinstance(active, dict):
            raise HTTPException(status_code=409, detail="A game invite requires an active session.")
        game_session_id = int(active.get("GameSessionId") or 0)
        with context.db.connection() as conn:
            instances = _read_game_instances(conn)
            instance = next((item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id), None)
            if instance is None:
                raise HTTPException(status_code=409, detail="Active game session is unavailable.")
            await _record_instance_invites(
                instance, source_id, [target_id], context
            )
            _write_game_instances(conn, instances)
        if room_id <= 0:
            room_id = int(active.get("RoomId") or active.get("RecRoomId") or 0)
    data = _BASE._str_field(payload, "Data", "data", default="")
    if message_type == 30 and not _user_text_is_pure(
            context,
            data,
            field_context="message.direct_text",
            player=player,
    ):
        return JSONResponse({"ChatMessage": None, "ChatResult": 6})
    message = await _create_recnet_message(
        target,
        from_player_id=source_id,
        message_type=message_type,
        data=data,
        room_id=room_id,
        context=context,
    )
    # Preserve the established response body for server-side invite tooling.
    return JSONResponse({"ChatMessage": message, "ChatResult": 0})


async def _handle_send_multiple_messages(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    raw_target_ids = payload.get("ToPlayerIds", payload.get("toPlayerIds", []))
    if not isinstance(raw_target_ids, list):
        raise HTTPException(status_code=400, detail="ToPlayerIds must be a list.")
    source_id = _legacy_id_for_player(player)
    target_ids = sorted(
        {
            int(value)
            for value in raw_target_ids
            if str(value).lstrip("-").isdigit() and int(value) > 0 and int(value) != source_id
        }
    )
    message_type = _BASE._int_field(payload, "Type", "type", default=-1)
    recognized = {0, 1, 2, 3, 4, 5, 10, 11, 20, 30, 40, 50, 51, 60, 61, 62, 70, 81, 100}
    if not target_ids or message_type not in recognized:
        raise HTTPException(status_code=400, detail="Valid ToPlayerIds and Type are required.")
    targets = []
    for target_id in target_ids:
        target = _find_player_by_legacy_id_25april2019(context, target_id)
        if target is None:
            raise HTTPException(status_code=404, detail=f"Target player {target_id} not found.")
        if _players_ignore_each_other(context, source_id, target_id):
            raise HTTPException(
                status_code=403,
                detail=f"Player {target_id} cannot receive messages from this player.",
            )
        targets.append((target_id, target))

    room_id = _BASE._int_field(payload, "RoomId", "roomId", default=0)
    if message_type == 0:
        active = await _active_game_session_for_player(player, context)
        if not isinstance(active, dict):
            raise HTTPException(status_code=409, detail="A game invite requires an active session.")
        game_session_id = int(active.get("GameSessionId") or 0)
        with context.db.connection() as conn:
            instances = _read_game_instances(conn)
            instance = next(
                (item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id),
                None,
            )
            if instance is None:
                raise HTTPException(status_code=409, detail="Active game session is unavailable.")
            await _record_instance_invites(instance, source_id, target_ids, context)
            _write_game_instances(conn, instances)
        if room_id <= 0:
            room_id = int(active.get("RoomId") or active.get("RecRoomId") or 0)

    data = _BASE._str_field(payload, "Data", "data", default="")
    if message_type == 30 and not _user_text_is_pure(
            context,
            data,
            field_context="message.direct_text",
            player=player,
    ):
        return Response(status_code=204)
    for _, target in targets:
        await _create_recnet_message(
            target,
            from_player_id=source_id,
            message_type=message_type,
            data=data,
            room_id=room_id,
            context=context,
        )
    # RecNet.Messages.SendMultiple uses an empty ApiCallback, not a response DTO.
    return Response(status_code=204)


async def _handle_offline_invite(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    target_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target_id <= 0 or target is None:
        raise HTTPException(status_code=404, detail="Target player not found.")
    source_id = _legacy_id_for_player(player)
    if target_id == source_id:
        raise HTTPException(status_code=400, detail="A player cannot invite themselves.")
    active = await _active_game_session_for_player(player, context)
    if not isinstance(active, dict):
        raise HTTPException(status_code=409, detail="A game invite requires an active session.")
    game_session_id = int(active.get("GameSessionId") or 0)
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        instance = next(
            (item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id),
            None,
        )
        if instance is None:
            raise HTTPException(status_code=409, detail="Active game session is unavailable.")
        await _record_instance_invites(instance, source_id, [target_id], context)
        _write_game_instances(conn, instances)
    await _create_recnet_message(
        target,
        from_player_id=source_id,
        message_type=0,
        room_id=int(active.get("RoomId") or active.get("RecRoomId") or 0),
        context=context,
    )
    return JSONResponse({"Message": "Invite sent."})


async def _handle_is_pure_string(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    value = payload.get("Value", payload.get("value"))
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="Value must be a string.")
    # This HTTP result is the only server-side gate for Photon chat emotes.
    return JSONResponse({
        "IsPure": _user_text_is_pure(
            context,
            value,
            field_context="photon.chat_emote",
            player=player,
        )
    })


async def _handle_moderation_block_details(request: Request, context) -> Response:
    row = context.player_from_request(request, API_VERSION)
    if row is None:
        raise HTTPException(status_code=401, detail="Authenticated player is required.")
    player = dict(row)
    try:
        player["state"] = json.loads(player.get("state_json") or "{}")
    except Exception:
        player["state"] = {}
    sanction = context.active_player_sanction(str(player["player_id"]))
    is_ban = bool(player.get("is_banned", False)) or (
        sanction is not None and str(sanction.get("sanction_type")) == "ban"
    )
    if sanction is None and not is_ban:
        return JSONResponse(
            {
                "ReportCategory": 0,
                "Duration": 0,
                "GameSessionId": 0,
                "IsHostKick": False,
                "PlayerIdReporter": None,
                "Message": "",
                "IsBan": False,
            }
        )
    duration = 0
    if sanction is not None and sanction.get("expires_at"):
        try:
            expires_at = datetime.fromisoformat(
                str(sanction["expires_at"]).replace("Z", "+00:00")
            ).astimezone(timezone.utc)
            duration = max(
                0,
                int((expires_at - datetime.now(timezone.utc)).total_seconds() + 0.999999),
            )
        except ValueError:
            duration = 0
    active = _player_state(player).get("game_session")
    return JSONResponse(
        {
            "ReportCategory": 0,
            "Duration": duration,
            "GameSessionId": (
                int(active.get("GameSessionId") or 0)
                if isinstance(active, dict)
                else 0
            ),
            "IsHostKick": False,
            "PlayerIdReporter": None,
            "Message": str(
                (sanction or {}).get("reason")
                or player.get("ban_reason")
                or ("This account is banned." if is_ban else "This account is temporarily restricted.")
            ),
            "IsBan": is_ban,
        }
    )


async def _handle_create_player_report(request: Request, context) -> Response:
    """Persist a v3 allegation without treating it as proof or punishment."""
    reporter = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    reporter_id = _legacy_id_for_player(reporter)
    target_id = _BASE._int_field(
        payload, "PlayerIdReported", "playerIdReported", default=0
    )
    category = _BASE._int_field(payload, "ReportCategory", "reportCategory", default=0)
    details = _BASE._str_field(payload, "Details", "details", default="")[:2000]
    valid_categories = {-1, 0, 1, 2, 3, 4, 5, 6, 7, 10, 100, 101, 102, 103, 104, 1000}
    if (
        target_id <= 0
        or target_id == reporter_id
        or category not in valid_categories
        or _find_player_by_legacy_id_25april2019(context, target_id) is None
    ):
        return JSONResponse({"Success": False, "Message": "Invalid player report."})

    game_session = await context.require_transient().get_membership(reporter_id)
    room_id = (
        int(game_session.get("RoomId") or 0)
        if isinstance(game_session, dict)
        else _BASE._int_field(payload, "RoomId", "roomId", default=0)
    )
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    assert target is not None
    game_session_id = (
        int(game_session.get("GameSessionId") or 0)
        if isinstance(game_session, dict)
        else 0
    )
    _submit_canonical_report(
        reporter=reporter,
        target_type="player",
        target_id=target["player_id"],
        raw_category=category,
        canonical_category=PLAYER_REPORT_CATEGORY_MAP.get(category, "unknown"),
        category_schema="player_reporting_v3",
        details=details,
        room_id=room_id,
        game_session_id=game_session_id,
        source_endpoint="api/PlayerReporting/v3/create",
        source_payload=payload,
        context=context,
    )
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_hile_warning(request: Request, context) -> Response:
    """Record PlayerReporting.CreateHileWarning telemetry from the exact client."""
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    warning_type = _BASE._int_field(payload, "Type", "type", default=-1)
    message = _BASE._str_field(payload, "Message", "message", default="").strip()[:2000]
    # PlayerReporting.HileType: Obcured, Time, Inject, GiftCount, Engine.
    if warning_type not in {0, 1, 2, 3, 4}:
        return JSONResponse({"Success": False, "Message": "Invalid Hile warning type."})
    warnings = _BASE._get_json_setting(context, f"{API_VERSION}:hile_warnings", [])
    if not isinstance(warnings, list):
        warnings = []
    warnings.append(
        {
            "PlayerId": _legacy_id_for_player(player),
            "Type": warning_type,
            "Message": message,
            "CreatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )
    _BASE._set_json_setting(context, f"{API_VERSION}:hile_warnings", warnings[-1000:])
    return JSONResponse({"Success": True, "Message": ""})


async def _handle_ban_player_v2(request: Request, context) -> Response:
    """Apply the exact room-local owner ban; never turn it into a global ban."""
    source = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    source_id = _legacy_id_for_player(source)
    target_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    if target_id <= 0 or target_id == source_id:
        raise HTTPException(status_code=400, detail="A different PlayerId is required.")
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Player not found.")
    if bool(target.get("is_coach", False)) or target_id == 1:
        raise HTTPException(status_code=403, detail="Coach cannot be banned.")

    display_reason = _BASE._str_field(
        payload,
        "DisplayReason",
        "displayReason",
        default="Banned by a player-room owner.",
    ).strip()[:512] or "Banned by a player-room owner."
    display_reason = _filter_user_text(
        context,
        display_reason,
        policy="censor",
        field_context="room.local_ban.display_reason",
        player=source,
    )
    active = await _active_game_session_for_player(source, context)
    game_session_id = int(active.get("GameSessionId") or 0) if isinstance(active, dict) else 0
    if game_session_id <= 0:
        raise HTTPException(status_code=409, detail="You are not in a game session.")

    members = {
        int(value)
        for value in await context.require_transient().session_member_ids(
            game_session_id
        )
        if str(value).lstrip("-").isdigit()
    }
    if source_id not in members or target_id not in members:
        raise HTTPException(status_code=409, detail="Both players must be in the same room.")

    with context.db.transaction() as conn:
        instances = _read_game_instances(conn)
        instance = next(
            (
                item
                for item in instances
                if int(item.get("GameSessionId") or 0) == game_session_id
            ),
            None,
        )
        if instance is None:
            raise HTTPException(status_code=409, detail="Game session not found.")
        room_id = int(instance.get("RoomId") or 0)
        dorm = _find_dorm_room_by_room_id(context, room_id)
        ugc = _find_ugc_room(context, room_id=room_id)
        owns_room = bool(
            (dorm is not None and dorm["row"]["owner_player_id"] == source["player_id"])
            or (ugc is not None and ugc["row"]["owner_player_id"] == source["player_id"])
        )
        if not owns_room:
            raise HTTPException(status_code=403, detail="Only the player-room owner can ban players.")
        owned_record = dorm if dorm is not None else ugc
        assert owned_record is not None
        metadata = owned_record["metadata"]
        banned_ids = {
            int(value) for value in metadata.get("banned_player_ids", [])
            if str(value).lstrip("-").isdigit()
        }
        banned_ids.add(target_id)
        metadata["banned_player_ids"] = sorted(banned_ids)
        conn.execute(
            """
            UPDATE rooms
            SET metadata_json = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE room_id = ?
            """,
            (json.dumps(metadata, sort_keys=True), owned_record["row"]["room_id"]),
        )
        for item in instances:
            if int(item.get("RoomId") or 0) == room_id:
                item["banned"] = sorted(banned_ids)
        _write_game_instances(conn, instances)

    await context.require_transient().consume_session_invite(
        game_session_id, target_id
    )

    await _finalize_session_kick(
        target_id,
        game_session_id,
        reporter_id=source_id,
        context=context,
        is_ban=True,
    )
    # Moderation.LocalRequestPlayerBan uses Core.ApiCallback and expects an
    # empty successful response.
    return Response(status_code=204)


async def _finalize_session_kick(
    target_id: int,
    game_session_id: int,
    *,
    reporter_id: int | None,
    context,
    is_ban: bool = False,
    server_wide: bool = False,
    is_host_kick: bool = True,
    duration_seconds: int = 0,
    message: str | None = None,
    queue_if_offline: bool = True,
) -> None:
    target = _find_player_by_legacy_id_25april2019(context, target_id)
    if target is not None:
        await context.require_transient().remove_session_member(
            game_session_id, target_id
        )
        await _send_hub_notification(
            target_id,
            22,
            {
                "ReportCategory": -1,
                "Duration": max(0, int(duration_seconds)),
                "GameSessionId": game_session_id,
                "PlayerIdReporter": reporter_id,
                "Message": message or (
                    "You were banned from this server."
                    if is_ban and server_wide
                    else "You were banned from this room."
                    if is_ban
                    else "You were removed from this game session."
                ),
                "IsHostKick": is_host_kick,
                "IsBan": is_ban,
            },
            context=context,
            queue_if_offline=queue_if_offline,
        )
        _schedule_presence_update(target_id, context)


async def _detach_player_from_game_instances(
    player_id: int,
    game_session_id: int,
    context,
) -> bool:
    """Remove a player from every instance and report whether the active one was a dorm."""
    active_was_dorm = False
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        active_was_dorm = any(
            int(instance.get("GameSessionId") or 0) == game_session_id
            and bool(instance.get("is_dorm", False))
            for instance in instances
        )
    if not active_was_dorm:
        await context.require_transient().remove_session_member(
            game_session_id, player_id
        )
    return active_was_dorm


async def _eject_connected_player_to_dorm(
    player_id: int,
    *,
    context,
    is_ban: bool,
    duration_seconds: int,
    message: str,
) -> dict[str, Any]:
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        return {"status": "player_not_found", "delivered": False}
    active = await context.require_transient().get_membership(player_id)
    game_session_id = (
        int(active.get("GameSessionId") or 0)
        if isinstance(active, dict)
        else 0
    )
    if game_session_id <= 0:
        return {"status": "no_active_room", "delivered": False}
    if await _detach_player_from_game_instances(player_id, game_session_id, context):
        return {"status": "already_in_dorm", "delivered": False}
    connected = await context.require_transient().route_player_online(
        API_VERSION, HUB_TRANSPORT, player_id
    )
    await _finalize_session_kick(
        player_id,
        game_session_id,
        reporter_id=None,
        context=context,
        is_ban=is_ban,
        server_wide=True,
        is_host_kick=False,
        duration_seconds=duration_seconds,
        message=message,
        queue_if_offline=False,
    )
    return {
        "status": "ejected" if connected else "detached_without_hub",
        "delivered": connected,
        "game_session_id": game_session_id,
    }


async def enforce_account_room_lock(
    *,
    player_id: str,
    sanction_type: str,
    reason: str,
    duration_seconds: int | None,
    context,
) -> dict[str, Any]:
    with context.db.connection() as conn:
        row = conn.execute(
            """
            SELECT p.*, pvs.state_json
            FROM players AS p
            JOIN player_version_state AS pvs ON p.player_id = pvs.player_id
            WHERE p.player_id = ? AND pvs.api_version = ?
            LIMIT 1
            """,
            (player_id, API_VERSION),
        ).fetchone()
    if row is None:
        return {"status": "player_not_in_adapter", "delivered": False}
    player = dict(row)
    try:
        player["state"] = json.loads(player.get("state_json") or "{}")
    except Exception:
        player["state"] = {}
    legacy_id = _legacy_id_for_player(player)
    if legacy_id <= 0:
        return {"status": "legacy_player_id_missing", "delivered": False}
    default_message = (
        "You were banned from this server."
        if sanction_type == "ban"
        else "Your account has been temporarily restricted."
    )
    result = await _eject_connected_player_to_dorm(
        legacy_id,
        context=context,
        is_ban=sanction_type == "ban",
        duration_seconds=max(0, int(duration_seconds or 0)),
        message=str(reason or default_message),
    )
    return {"legacy_player_id": legacy_id, **result}


async def _handle_vote_to_kick(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    source_id = _legacy_id_for_player(player)
    target_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
    game_session_id = _BASE._int_field(
        payload, "GameSessionId", "gameSessionId", default=0
    )
    if game_session_id <= 0:
        active = await _active_game_session_for_player(player, context)
        game_session_id = int(active.get("GameSessionId") or 0) if isinstance(active, dict) else 0
    raw_response = payload.get("Response", payload.get("response", False))
    response_yes = (
        raw_response.strip().casefold() in {"true", "1"}
        if isinstance(raw_response, str)
        else bool(raw_response)
    )
    if target_id <= 0 or game_session_id <= 0 or target_id == source_id:
        return JSONResponse({"Success": False, "Message": "Invalid vote-to-kick request."})

    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        instance = next(
            (item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id),
            None,
        )
        if instance is None:
            return JSONResponse({"Success": False, "Message": "Game session not found."})
    vote_status, _votes, _required = await context.require_transient().vote_to_kick(
        game_session_id=game_session_id,
        voter_id=source_id,
        target_id=target_id,
        vote_yes=response_yes,
    )
    if vote_status == "not_members":
        return JSONResponse({"Success": False, "Message": "Both players must be in the game session."})
    if vote_status == "kicked":
        await _finalize_session_kick(
            target_id, game_session_id, reporter_id=source_id, context=context
        )
        return JSONResponse({"Success": True, "Message": "Player removed from the game session."})
    return JSONResponse({"Success": True, "Message": "Vote recorded."})


async def _handle_instant_kick(request: Request, context) -> Response:
    player = _authenticated_player(request, context)
    payload = await _BASE._parse_client_payload(request)
    source_id = _legacy_id_for_player(player)
    game_session_id = _BASE._int_field(
        payload, "GameSessionId", "gameSessionId", default=0
    )
    if game_session_id <= 0:
        active = await _active_game_session_for_player(player, context)
        game_session_id = int(active.get("GameSessionId") or 0) if isinstance(active, dict) else 0
    raw_target_ids = payload.get("PlayerIds", payload.get("playerIds"))
    if raw_target_ids is None:
        singular_id = _BASE._int_field(payload, "PlayerId", "playerId", default=0)
        raw_target_ids = [singular_id] if singular_id > 0 else []
    if not isinstance(raw_target_ids, list):
        return JSONResponse({"Success": False, "Message": "PlayerIds must be a list."})
    target_ids = {
        int(value)
        for value in raw_target_ids
        if str(value).lstrip("-").isdigit() and int(value) > 0 and int(value) != source_id
    }
    if game_session_id <= 0 or not target_ids:
        return JSONResponse({"Success": False, "Message": "An active session and target player are required."})
    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
        instance = next(
            (item for item in instances if int(item.get("GameSessionId") or 0) == game_session_id),
            None,
        )
        if instance is None:
            return JSONResponse({"Success": False, "Message": "Game session not found."})
        members = [
            int(value)
            for value in await context.require_transient().session_member_ids(
                game_session_id
            )
            if str(value).lstrip("-").isdigit()
        ]
        # The first admitted member is the Photon master/host represented by
        # this service. InstantKick is a host action, never a global ban.
        try:
            host_id = int(
                await context.require_transient().session_host_id(game_session_id)
                or 0
            )
        except (TypeError, ValueError):
            host_id = 0
        if not members or host_id != source_id:
            return JSONResponse({"Success": False, "Message": "Only the session host can instantly kick."})
        removed = [value for value in members if value in target_ids]
    for target_id in removed:
        await _finalize_session_kick(
            target_id, game_session_id, reporter_id=source_id, context=context
        )
    return JSONResponse(
        {
            "Success": bool(removed),
            "Message": "Player removed from the game session." if removed else "No matching player was in the session.",
        }
    )


def _clean_route_path(route_path: str) -> str:
    return route_path.split("?", 1)[0].strip("/")


async def _create_hub_connection_id(player, context) -> str:
    player_id = _legacy_id_for_player(player)
    if player_id <= 0:
        raise HTTPException(status_code=401, detail="Authenticated player is required.")
    signing_secret = await context.require_transient().get_or_create_secret(
        f"{API_VERSION}-hub",
        player_id,
        ttl_seconds=7 * 24 * 60 * 60,
    )
    issued_at = int(datetime.now(timezone.utc).timestamp())
    nonce = secrets.token_hex(12)
    signed_value = f"{player_id}:{issued_at}:{nonce}"
    signature = hmac.new(signing_secret.encode("utf-8"), signed_value.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return f"rr25-{player_id}-{issued_at}-{nonce}-{signature}"


async def _player_from_hub_connection_id(context, connection_id: str) -> dict[str, Any] | None:
    parts = str(connection_id or "").split("-", 4)
    if len(parts) != 5 or parts[0] != "rr25":
        return None
    try:
        player_id = int(parts[1])
        issued_at = int(parts[2])
    except ValueError:
        return None
    now = int(datetime.now(timezone.utc).timestamp())
    if issued_at > now + 30 or now - issued_at > HUB_NEGOTIATION_MAX_AGE_SECONDS:
        return None
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        return None
    signing_secret = await context.require_transient().get_secret(
        f"{API_VERSION}-hub", player_id
    )
    if not signing_secret:
        return None
    signed_value = f"{player_id}:{issued_at}:{parts[3]}"
    expected = hmac.new(signing_secret.encode("utf-8"), signed_value.encode("utf-8"), hashlib.sha256).hexdigest()[:32]
    return player if hmac.compare_digest(expected, parts[4]) else None


def allow_sanctioned_http_route(*, route_path: str, method: str) -> bool:
    clean_path = _clean_route_path(route_path).casefold()
    if (
        method.upper() == "GET"
        and clean_path == "api/playerreporting/v1/moderationblockdetails"
    ):
        return True
    return method.upper() == "POST" and clean_path in {
        "api/gamesessions/v3/joinplayer",
        "api/gamesessions/v3/joinplayerevent",
        "api/gamesessions/v4/joinroom",
        "api/gamesessions/v3/joininstance",
    }


async def handle_http(*, request: Request, route_path: str, context) -> Response:
    clean_path = _clean_route_path(route_path)
    path = clean_path.casefold()
    method = request.method.upper()

    video_match = re.fullmatch(
        r"(?:api/communityboard/)?video/([^/]+)",
        clean_path,
        flags=re.IGNORECASE,
    )
    if video_match:
        if method in {"GET", "HEAD"}:
            return await _handle_community_board_video(video_match.group(1))
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    # 1. Name Server root request
    if path == "" or path == "/":
        api_base_url = context.public_api_base_url(request, API_VERSION)
        websocket_base_url = context.public_websocket_base_url(request, API_VERSION)

        return JSONResponse({
            "Auth": api_base_url,
            "API": api_base_url,
            "WWW": api_base_url,
            "Notifications": websocket_base_url,
            "Images": api_base_url,
            "Commerce": api_base_url,
        })

    # 2. Version Check
    if path == "api/versioncheck/v3":
        return JSONResponse({"ValidVersion": True})

    # 3. Config Settings
    if path == "api/config/v2":
        return await _handle_config_v2(request, context)

    # 3.5 Game Configs
    if path == "api/gameconfigs/v1/all":
        return await _handle_gameconfigs_all(request, context)

    # 4. Login and handshakes
    if path in {"api/platformlogin/v1", "api/platformlogin/v2", "api/platformlogin/v2/"}:
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_platform_login(request, context)

    if path in {"api/accounts/v2/getcachedlogins", "api/platformlogin/v2/getcachedlogins"}:
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_get_cached_logins(request, context)

    if path == "api/platformlogin/v1/refreshlogin":
        if method == "GET":
            return await _handle_refresh_login(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/platformlogin/v1/removecachedlogin":
        if method == "POST":
            return await _handle_remove_cached_login(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/accounts/v2/login":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_login_v2(request, context)

    if path == "api/accounts/v3/password/recover":
        if method == "POST":
            return await _handle_password_recovery(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path in {"api/accounts/v1/create", "api/accounts/v1/createprofile"}:
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_create_account(request, context)

    if path == "api/platformlogin/v2/createaccount":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_create_account_v2(request, context)

    if path == "api/platformlogin/v2/loginaccount":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_login_account_v2(request, context)

    if path == "api/platformlogin/v1/registeraccount":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_register_account(request, context)

    if path == "api/platformlogin/v2/logincached":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_login_cached_v2(request, context)

    if path == "api/players/v1/list":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_profile_list(request, context)

    if path == "api/players/v1/createprofile":
        if method == "POST":
            return await _handle_create_profile(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v1/deleteprofile":
        if method == "POST":
            return await _handle_delete_profile(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v1/birthday":
        if method == "POST":
            return await _handle_birthday_update(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v1/getgeneratednameoptions":
        if method == "GET":
            return await _handle_generated_name_options(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v2/updatereputation":
        if method == "POST":
            return await _handle_reputation_update(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerreputation/v1/heal":
        if method == "POST":
            return await _handle_reputation_heal(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v2/listbyplatformid":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_profiles_by_platform_ids(request, context)

    if path == "api/players/v2/search":
        if method != "GET":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_profile_name_lookup(request, context, search=True)

    if path == "api/players/v1/getbyusername":
        if method != "GET":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_profile_name_lookup(request, context, search=False)

    if path == "api/players/v1/disallowinapppurchases":
        if method != "GET":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_disallow_in_app_purchases(request, context)

    if path == "api/players/v1/objectives":
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_player_objective_completions(request, context)

    if path == "api/players/v2/displayname":
        if method == "POST":
            return await _handle_display_name(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v1/bio":
        if method == "POST":
            return await _handle_bio_update(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playercheer/v1/setselectedcheer":
        if method == "POST":
            return await _handle_update_selected_cheer(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playercheer/v1/create":
        if method == "POST":
            return await _handle_create_cheer(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v2/phone":
        if method == "POST":
            return await _handle_phone_update(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v2/phone/verify":
        if method == "POST":
            return await _handle_phone_verify(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v1/phonelastfour":
        if method == "GET":
            return await _handle_phone_last_four(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/players/v1/avoidjuniors":
        if method == "POST":
            return await _handle_avoid_juniors(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path.startswith("api/players/v1/"):
        raw_player_id = path.rsplit("/", 1)[1]
        if raw_player_id.isdigit():
            if method != "GET":
                raise HTTPException(status_code=405, detail="Method Not Allowed")
            return await _handle_profile_by_id(int(raw_player_id), context)

    # 5. Settings
    if path == "api/settings/v2":
        if method == "GET":
            return await _handle_get_settings_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/settings/v2/set":
        if method == "POST":
            return await _handle_set_setting_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/settings/v2/remove":
        if method == "POST":
            return await _handle_remove_setting_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    # 6. Avatar
    if path == "api/avatar/v2":
        if method == "GET":
            return await _handle_get_avatar_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/avatar/v2/set":
        if method == "POST":
            return await _handle_set_avatar_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/avatar/v3/saved":
        if method == "GET":
            return await _handle_get_saved_outfits_v3(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/avatar/v3/saved/set":
        if method == "POST":
            return await _handle_set_saved_outfit_v3(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/avatar/v3/items":
        if method == "GET":
            return await _handle_get_unlocked_items_v3(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/avatar/v2/gifts":
        if method == "GET":
            return await _handle_get_gifts_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path in {"api/avatar/v2/gifts/consume", "api/avatar/v2/gifts/consume/"}:
        if method == "POST":
            return await _handle_consume_gift_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path in {"api/leaderboard/v1", "api/leaderboard/v1/"}:
        if method == "POST":
            return await _handle_leaderboard_v1(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    # 7. Presence, Relationships, Messages
    if path == "api/presence/v3/heartbeat":
        if method not in {"GET", "POST"}:
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_presence_heartbeat(request, context)

    if path == "api/presence/v2/list":
        if method == "POST":
            return await _handle_presence_list(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/relationships/v2/get":
        if method == "GET":
            return await _handle_get_relationships_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    relationship_type_match = re.fullmatch(
        r"api/relationships/v2/(addfriend|removefriend|sendfriendrequest|acceptfriendrequest)", path
    )
    if relationship_type_match:
        if method != "GET":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        target_id = _BASE._int_field(dict(request.query_params), "id", "Id", default=0)
        if target_id <= 0:
            raise HTTPException(status_code=400, detail="id is required.")
        return await _handle_relationship_type_change(
            request, context, target_id, relationship_type_match.group(1)
        )

    relationship_favorite_match = re.fullmatch(
        r"api/relationships/v1/(favorite|unfavorite)", path
    )
    if relationship_favorite_match:
        if method != "GET":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        target_id = _BASE._int_field(dict(request.query_params), "id", "Id", default=0)
        if target_id <= 0:
            raise HTTPException(status_code=400, detail="id is required.")
        return await _handle_relationship_flag_change(
            request,
            context,
            target_id,
            "Favorited",
            relationship_favorite_match.group(1) == "favorite",
        )

    relationship_preference_match = re.fullmatch(
        r"api/relationships/v1/(mute|unmute|ignore|unignore)", path
    )
    if relationship_preference_match:
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        action = relationship_preference_match.group(1)
        return await _handle_relationship_flag_post(
            request,
            context,
            "Muted" if action in {"mute", "unmute"} else "Ignored",
            action in {"mute", "ignore"},
        )

    if path == "api/messages/v2/get":
        if method == "GET":
            return await _handle_get_messages_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/messages/v2/send":
        if method == "POST":
            return await _handle_send_message(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/messages/v2/delete":
        if method == "POST":
            return await _handle_delete_message_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/messages/v1/sendmultiple":
        if method == "POST":
            return await _handle_send_multiple_messages(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/offlineinvite/v1/send":
        if method == "POST":
            return await _handle_offline_invite(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    # 8. Rooms
    if path == "api/rooms/v2/myrooms":
        if method == "GET":
            return await _handle_get_rooms_mycreated_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/mybookmarkedrooms":
        if method == "GET":
            return await _handle_get_rooms_bookmarked_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/myrecent":
        if method == "GET":
            return await _handle_get_rooms_recent_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/mysubscriptions":
        if method == "GET":
            return await _handle_get_rooms_subscribed_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/baserooms":
        if method == "GET":
            return await _handle_get_base_rooms(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/bookmark":
        if method == "POST":
            return await _handle_bookmark_room(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/cheer":
        if method == "POST":
            return await _handle_cheer_room(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/filters":
        if method == "GET":
            return await _handle_room_filters(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/search":
        if method == "GET":
            return await _handle_search_rooms(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/clone":
        if method == "POST":
            return await _handle_clone_room(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/modify":
        if method == "POST":
            return await _handle_modify_room(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/modifypermissions":
        if method == "POST":
            return await _handle_modify_room_permissions(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v2/report":
        if method == "POST":
            return await _handle_report_room(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/modify/tags":
        if method == "POST":
            return await _handle_modify_room_tags(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/modify/sceneparent":
        if method == "POST":
            return await _handle_modify_scene_parent(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/modifyscene":
        if method == "POST":
            return await _handle_modify_room_scene(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v3/featured":
        if method == "GET":
            return await _handle_get_rooms_featured_v3(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/featuredroomgroup":
        if method == "GET":
            return await _handle_get_featured_room_group_v1(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/hot":
        if method == "GET":
            return await _handle_get_rooms_hot_v1(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/live":
        if method == "GET":
            return await _handle_get_rooms_live_v1(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/agroomids":
        if method == "GET":
            return await _handle_get_ag_room_ids_v1(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    save_data_match = re.fullmatch(r"api/rooms/v3/savedata/(\d+)", path)
    if save_data_match:
        if method == "POST":
            return await _handle_save_room_data(int(save_data_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    room_data_history_match = re.fullmatch(r"api/rooms/v1/datahistory/(\d+)", path)
    if room_data_history_match:
        if method == "GET":
            return await _handle_room_data_history(
                int(room_data_history_match.group(1)), request, context
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/rooms/v1/datahistory/restore":
        if method == "POST":
            return await _handle_restore_room_data_history(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/creatorids":
        if method == "GET":
            return await _handle_invention_creator_ids(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    room_data_match = re.fullmatch(r"room/([a-z0-9._-]+)", path)
    if room_data_match:
        if method == "GET":
            return await _handle_get_room_data(room_data_match.group(1), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    cdn_blob_match = re.fullmatch(r"(?:data|invention)/([a-z0-9._-]+)", path)
    if cdn_blob_match:
        if method == "GET":
            return await _handle_get_room_data(cdn_blob_match.group(1), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if re.fullmatch(
        rf"{API_VERSION}-invention-\d+(?:-v\d+)?-[0-9a-f]+\.blob", path
    ):
        if method == "GET":
            return await _handle_get_room_data(path, request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path.startswith("api/rooms/v2/name/"):
        if method == "GET":
            return await _handle_get_room_by_name_v2(path.rsplit("/", 1)[1], request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    room_by_id_v2_match = re.fullmatch(r"api/rooms/v2/(\d+)", path)
    if room_by_id_v2_match:
        if method == "GET":
            return await _handle_get_room_by_id_v2(int(room_by_id_v2_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path.startswith("api/rooms/v4/details/"):
        raw_room_id = path.rsplit("/", 1)[1]
        if method == "GET" and raw_room_id.isdigit():
            return await _handle_get_room_details_v4(int(raw_room_id), request, context)
        raise HTTPException(status_code=405 if method != "GET" else 404, detail="Room details not found.")

    if path.startswith("api/rooms/v2/personaldetails/"):
        raw_room_id = path.rsplit("/", 1)[1]
        if method == "GET" and raw_room_id.isdigit():
            return await _handle_get_room_personal_details_v2(int(raw_room_id), request, context)
        raise HTTPException(status_code=405 if method != "GET" else 404, detail="Room not found.")

    if path.startswith("api/rooms/v2/instancedetails/"):
        raw_room_id = path.rsplit("/", 1)[1]
        if method == "GET" and raw_room_id.isdigit():
            return await _handle_get_room_instance_details_v2(int(raw_room_id), request, context)
        raise HTTPException(status_code=405 if method != "GET" else 404, detail="Room not found.")

    if path == "api/gamesessions/v3/joinplayer":
        if method == "POST":
            return await _handle_join_player(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/gamesessions/v3/joinplayerevent":
        if method == "POST":
            return await _handle_join_player_event(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/gamesessions/v4/joinroom":
        if method == "POST":
            return await _handle_join_room(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/gamesessions/v3/joininstance":
        if method == "POST":
            return await _handle_join_instance(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/gamesessions/v2/setinprogress":
        if method == "POST":
            return await _handle_set_game_in_progress(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/gamesessions/v2/reportjoinresult":
        if method == "POST":
            return await _handle_report_game_join_result(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    group_memberships_match = re.fullmatch(r"api/groups/v1/memberships/(\d+)", path)
    if group_memberships_match:
        if method == "GET":
            return await _handle_group_memberships(int(group_memberships_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/groups/v1":
        if method == "POST":
            return await _handle_create_group(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    group_delete_match = re.fullmatch(r"api/groups/v1/delete/(\d+)", path)
    if group_delete_match:
        if method == "POST":
            return await _handle_delete_group(int(group_delete_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    group_name_match = re.fullmatch(r"api/groups/v1/name/(.+)", path)
    if group_name_match:
        if method == "GET":
            return await _handle_get_group(request, context, group_name=group_name_match.group(1))
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    group_id_match = re.fullmatch(r"api/groups/v1/(\d+)", path)
    if group_id_match:
        if method == "GET":
            return await _handle_get_group(request, context, group_id=int(group_id_match.group(1)))
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/events/v3/list":
        if method == "GET":
            return await _handle_events_list(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    event_status_match = re.fullmatch(r"api/events/v1/status/(\d+)", path)
    if event_status_match:
        if method == "GET":
            return await _handle_event_status(int(event_status_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerelo/update":
        if method == "POST":
            return await _handle_player_elo_update(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/royale/v1/current":
        if method == "GET":
            return await _handle_rec_royale_progress(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/royale/v2/matchcomplete":
        if method == "POST":
            return await _handle_rec_royale_match_complete(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerreporting/v1/moderationblockdetails":
        if method == "GET":
            return await _handle_moderation_block_details(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerreporting/v3/create":
        if method == "POST":
            return await _handle_create_player_report(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerreporting/v2/votetokick":
        if method == "POST":
            return await _handle_vote_to_kick(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerreporting/v1/instantkick":
        if method == "POST":
            return await _handle_instant_kick(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerreporting/v1/hile":
        if method == "POST":
            return await _handle_hile_warning(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playersbanned/v2/ban":
        if method == "POST":
            return await _handle_ban_player_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/platformlogin/v1/logout":
        if method == "POST":
            return await _handle_logout(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "hub/v1/negotiate":
        if method == "POST":
            player = _authenticated_player(request, context)
            return JSONResponse({
                "connectionId": await _create_hub_connection_id(player, context),
                "availableTransports": [
                    {
                        "transport": "WebSockets",
                        "transferFormats": ["Text", "Binary"]
                    }
                ]
            })
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/checklist/v1/current":
        if method == "GET":
            return await _handle_current_checklist(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/objectives/v1/myprogress":
        if method == "GET":
            return await _handle_objective_progress(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/objectives/v1/cleargroup":
        if method == "POST":
            return await _handle_clear_objective_group(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path in {"api/challenge/v1/getcurrent", "api/challenge/v1/getnext"}:
        if method != "GET":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        player = _authenticated_player(request, context)
        return JSONResponse(
            _weekly_challenge_response(
                player,
                context,
                next_week=path.endswith("getnext"),
            )
        )

    if path == "api/challenge/v2/updateprogress":
        if method == "POST":
            return await _handle_update_weekly_challenge_progress(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/presence/v1/setplayertype":
        if method == "POST":
            return await _handle_presence_state(request, context, "player_type", "PlayerType", "playerType")
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/presence/v1/setplayerstatusvisibility":
        if method == "POST":
            return await _handle_presence_state(
                request,
                context,
                "status_visibility",
                "StatusVisibility",
                "statusVisibility",
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/presence/v1/playerdisconnected":
        if method == "POST":
            return await _handle_presence_disconnected(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/chat/v2/mychats":
        if method == "GET":
            return await _handle_get_chat_threads(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/chat/v2/getbyplayers":
        if method == "GET":
            return await _handle_get_chat_by_players(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/chat/v2/getbyid":
        if method == "GET":
            return await _handle_get_chat_by_id(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/chat/v2/messages":
        if method == "GET":
            return await _handle_get_chat_messages(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/chat/v1/create":
        if method == "POST":
            return await _handle_create_chat(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    chat_mutations = {
        "api/chat/v1/sendmessage": _handle_send_chat_message,
        "api/chat/v1/readmessage": _handle_read_chat_message,
        "api/chat/v1/addtochat": _handle_add_to_chat,
        "api/chat/v1/leavechat": _handle_leave_chat,
        "api/chat/v1/renamechat": _handle_rename_chat,
        "api/chat/v1/snoozechat": _handle_snooze_chat,
    }
    if path in chat_mutations:
        if method == "POST":
            return await chat_mutations[path](request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playersubscriptions/v1/my":
        if method == "GET":
            return await _handle_get_player_subscriptions(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    subscription_match = re.fullmatch(r"api/playersubscriptions/v1/(subscribe|unsubscribe)/(\d+)", path)
    if subscription_match:
        if method != "POST":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return await _handle_player_subscription_change(
            request,
            context,
            int(subscription_match.group(2)),
            subscribe=subscription_match.group(1) == "subscribe",
        )

    if path == "api/config/v1/amplitude":
        if method == "GET":
            amplitude_key = context.get_server_setting("amplitude_key", "")
            return JSONResponse({"AmplitudeKey": amplitude_key if isinstance(amplitude_key, str) else ""})
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "config/loadingscreentipdata":
        if method == "GET":
            return await _handle_loading_screen_tips(context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/activities/charades/v1/words":
        if method == "GET":
            return await _handle_charades_words(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/testcasemanagement/v1/testpasssummary":
        if method == "GET":
            return await _handle_test_pass_summaries(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    test_pass_match = re.fullmatch(r"api/testcasemanagement/v1/testpass/(\d+)", path)
    if test_pass_match:
        if method == "GET":
            return await _handle_get_test_pass(int(test_pass_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    test_case_mutation_match = re.fullmatch(
        r"api/testcasemanagement/v1/testcase/([^/]+)/(claim|unclaim|status)", path
    )
    if test_case_mutation_match:
        if method == "POST":
            return await _handle_mutate_test_case(
                test_case_mutation_match.group(1),
                test_case_mutation_match.group(2),
                request,
                context,
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    test_case_match = re.fullmatch(r"api/testcasemanagement/v1/testcase/([^/]+)", path)
    if test_case_match:
        if method == "GET":
            return await _handle_get_test_case(test_case_match.group(1), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v1/listsaved":
        if method == "GET":
            return await _handle_list_saved_images(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v1/modifyaccessibility":
        if method == "POST":
            return await _handle_modify_saved_image_accessibility(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v1/deletesaved":
        if method == "POST":
            return await _handle_delete_image(
                request,
                context,
                expected_purpose=f"{API_VERSION}.saved_image",
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v1/sendlink":
        if method == "POST":
            return await _handle_saved_image_link(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v1/cheer":
        if method == "POST":
            return await _handle_cheer_image(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v2/deletetransient":
        if method == "POST":
            return await _handle_delete_image(
                request,
                context,
                expected_purpose=f"{API_VERSION}.transient_image",
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v2/named":
        if method == "GET":
            return await _handle_named_images(context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v1/slideshow":
        if method == "GET":
            return await _handle_current_slideshow(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "default_profile.png":
        if method != "GET":
            raise HTTPException(status_code=405, detail="Method Not Allowed")
        return Response(
            content=_BASE.DEFAULT_PROFILE_IMAGE_BYTES,
            media_type="image/png",
            headers={
                "Last-Modified": DEFAULT_PROFILE_IMAGE_LAST_MODIFIED,
                "Cache-Control": "no-cache, no-store, max-age=0",
            },
        )

    if path == "api/relationships/v1/bulkignoreplatformusers":
        if method == "POST":
            return await _handle_bulk_ignore_platform_users(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/communityboard/v1/current":
        if method == "GET":
            return await _handle_community_board(context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/quickplay/v1/getandclear":
        if method == "GET":
            return await _handle_quick_play(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerevents/v1/all":
        if method == "GET":
            return await _handle_player_events(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    # GetEventsURI in this exact client formats API + "/v1" while the API base
    # already ends in a slash, producing the observed double slash.
    if path in {"api/playerevents/v1", "api/playerevents//v1"}:
        if method == "GET":
            return await _handle_upcoming_player_events(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerevents/v2":
        if method == "POST":
            return await _handle_create_player_event(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerevents/v1/respond":
        if method == "POST":
            return await _handle_respond_to_player_event(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerevents/v1/deleteresponse":
        if method == "POST":
            return await _handle_delete_player_event_response(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerevents/v1/bulkinvite":
        if method == "POST":
            return await _handle_bulk_invite_to_player_event(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/playerevents/v1/report":
        if method == "POST":
            return await _handle_report_player_event(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    player_event_responses_match = re.fullmatch(r"api/playerevents/v1/(\d+)/responses", path)
    if player_event_responses_match:
        if method == "GET":
            return await _handle_get_player_event_responses(
                int(player_event_responses_match.group(1)), request, context
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    player_event_v1_match = re.fullmatch(r"api/playerevents/v1/(\d+)", path)
    if player_event_v1_match:
        if method == "GET":
            return await _handle_get_player_event(int(player_event_v1_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    player_event_delete_match = re.fullmatch(r"api/playerevents/v2/delete/(\d+)", path)
    if player_event_delete_match:
        if method == "POST":
            return await _handle_delete_player_event(int(player_event_delete_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    player_event_v2_match = re.fullmatch(r"api/playerevents/v2/(\d+)", path)
    if player_event_v2_match:
        if method == "GET":
            return await _handle_get_player_event(int(player_event_v2_match.group(1)), request, context)
        if method == "POST":
            return await _handle_modify_player_event(int(player_event_v2_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1":
        if method == "GET":
            return await _handle_get_invention(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/batch":
        if method == "POST":
            return await _handle_invention_batch(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/update":
        if method == "GET":
            return await _handle_update_invention(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/settags":
        if method == "POST":
            return await _handle_set_invention_tags(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/versions":
        if method == "GET":
            return await _handle_get_invention_versions(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v2/addversion":
        if method == "POST":
            return await _handle_add_invention_version(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    invention_lifecycle_routes = {
        "api/inventions/v1/delete": "delete",
        "api/inventions/v2/publish": "publish",
        "api/inventions/v1/unpublish": "unpublish",
        "api/inventions/v1/download": "download",
    }
    if path in invention_lifecycle_routes:
        if method == "GET":
            return await _handle_invention_lifecycle(
                invention_lifecycle_routes[path], request, context
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/report":
        if method == "POST":
            return await _handle_report_invention(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/cheer":
        if method == "POST":
            return await _handle_cheer_invention(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/details":
        if method == "GET":
            return await _handle_get_invention_details(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    invention_personal_details_match = re.fullmatch(
        r"api/inventions/v1/personaldetails/(\d+)", path
    )
    if invention_personal_details_match:
        if method == "GET":
            return await _handle_get_personal_invention_details(
                int(invention_personal_details_match.group(1)), request, context
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/version":
        if method == "GET":
            return await _handle_get_invention_version(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path in {"api/inventions/v1/mine", "api/inventions/v1/search"}:
        if method == "GET":
            return await _handle_invention_list(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v2/save":
        if method == "POST":
            return await _handle_save_invention(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/inventions/v1/tagfilters":
        if method == "GET":
            return await _handle_invention_filters(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path.startswith("api/storefronts/v3/giftdropstore/"):
        if method == "GET":
            _authenticated_player(request, context)
            raw_type = path.rsplit("/", 1)[1]
            if not raw_type.isdigit():
                raise HTTPException(status_code=404, detail="Gift-drop storefront not found.")
            return await _handle_gift_drop_storefront(int(raw_type), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    storefront_season_match = re.fullmatch(r"api/storefronts/v1/season/(\d+)", path)
    if storefront_season_match:
        if method == "GET":
            return await _handle_frontier_season(
                int(storefront_season_match.group(1)), request, context
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/storefronts/v2/buytier":
        if method == "POST":
            return await _handle_buy_frontier_tier(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/storefronts/v2/buyelite":
        if method == "POST":
            return await _handle_buy_frontier_elite(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/storefronts/v2/buyitem":
        if method == "POST":
            return await _handle_purchase_storefront_item(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/storefronts/v2/balance":
        if method == "POST":
            return await _handle_modify_storefront_balance(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/sanitize/v1/ispure":
        if method == "POST":
            return await _handle_is_pure_string(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    storefront_balance_match = re.fullmatch(r"api/storefronts/v4/balance/(\d+)", path)
    if storefront_balance_match:
        if method == "GET":
            return await _handle_storefront_balance(int(storefront_balance_match.group(1)), request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    balance_add_match = re.fullmatch(r"api/storefronts/v1/balanceaddtype/(\d+)/(\d+)", path)
    if balance_add_match:
        if method == "GET":
    # Path segments are currencyType followed by balanceAddType.
            return await _handle_balance_add_config(
                int(balance_add_match.group(2)),
                int(balance_add_match.group(1)),
                request,
                context,
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/storefronts/v1/objectives":
        if method == "POST":
            return await _handle_storefront_objective_completions(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/consumables/v1/getunlocked":
        if method == "GET":
            return await _handle_get_consumables(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/consumables/v1/consume":
        if method == "POST":
            return await _handle_consume_consumable(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/consumables/v1/updateactive":
        if method == "POST":
            return await _handle_update_active_consumable(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/checklist/v1/complete":
        if method == "POST":
            return await _handle_complete_checklist_item(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/objectives/v1/updateobjective":
        if method == "POST":
            return await _handle_update_objective(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/objectives/v1/completegroup":
        if method == "POST":
            return await _handle_complete_objective_group(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/avatar/v2/gifts/generate":
        if method == "POST":
            return await _handle_generate_gift_v2(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/equipment/v1/getunlocked":
        if method == "GET":
            return await _handle_get_equipment(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/equipment/v1/update":
        if method == "POST":
            return await _handle_update_equipment(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v4/uploadsaved":
        if method == "POST":
            return await _handle_upload_saved_image(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v5/uploadtransient":
        if method == "POST":
            return await _handle_upload_transient_image(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/images/v4/profile":
        if method == "POST":
            return await _handle_set_profile_image(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/datablob/v1/uploadtransient":
        if method == "POST":
            return await _handle_upload_transient_blob(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/catalog/v1/all":
        if method == "GET":
            _authenticated_player(request, context)
            return JSONResponse(_commerce_catalog(context))
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/purchase/v1/initiatepurchase":
        if method == "POST":
            return await _handle_initiate_commerce_purchase(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/purchase/v1/completepurchase":
        if method == "POST":
            return await _handle_finish_commerce_purchase(
                request, context, status="completed"
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/purchase/v1/cancelpurchase":
        if method == "POST":
            return await _handle_finish_commerce_purchase(
                request, context, status="cancelled"
            )
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/purchase/v1/cleanuppending":
        if method == "POST":
            return await _handle_cleanup_commerce_purchases(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if path == "api/bugreporting/v1/reportbug":
        if method == "POST":
            return await _handle_report_bug(request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    if (
        path.casefold() == DEFAULT_IMAGE_NAME.casefold()
        or any(path.casefold() == name.casefold() for name in COACH_ROOM_IMAGE_TEXTURES.values())
        or re.fullmatch(r"[0-9a-f-]{36}\.(?:jpg|jpeg|png)", path, flags=re.IGNORECASE)
        or re.fullmatch(
            r"CommunityBoardVideo_[A-Za-z0-9_-]+_[0-9a-f]{12}\.png",
            path,
            flags=re.IGNORECASE,
        )
    ):
        if method == "GET":
            return await _handle_uploaded_image(path, request, context)
        raise HTTPException(status_code=405, detail="Method Not Allowed")

    raise HTTPException(status_code=404, detail="Unknown endpoint.")


def _hub_notification_frame(notification_id: int, message: dict[str, Any]) -> str:
    notification = json.dumps(
        {"Id": str(notification_id), "Msg": message},
        separators=(",", ":"),
    )
    frame = json.dumps(
        {
            "type": 1,
            "target": "Notification",
            "arguments": [notification],
        },
        separators=(",", ":"),
    ) + "\x1e"
    return frame


def _pending_hub_notification_key(player_id: int, context) -> str | None:
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None or player_id == 1:
        return None
    return _canonical_player_setting_key("pending_hub_notifications", player["player_id"])


# Cache-replacement notifications are transient; event notifications are durable.
_NON_DURABLE_HUB_NOTIFICATION_IDS = {11, 12, 13, 15, 60, 61, 70, 71, 100}


def _queue_hub_notification(player_id: int, notification_id: int, message: dict[str, Any], context) -> None:
    if int(notification_id) in _NON_DURABLE_HUB_NOTIFICATION_IDS:
        return
    key = _pending_hub_notification_key(player_id, context)
    if key is None:
        return
    with _NOTIFICATION_LOCK:
        pending = _BASE._get_json_setting(context, key, [])
        if not isinstance(pending, list):
            pending = []
        pending.append({"Id": int(notification_id), "Msg": message})
        _BASE._set_json_setting(context, key, pending[-100:])


def _remove_pending_hub_notifications(
    player_id: int,
    context,
    *,
    notification_ids: set[int],
    message_id: int,
) -> None:
    key = _pending_hub_notification_key(player_id, context)
    if key is None:
        return
    with _NOTIFICATION_LOCK:
        pending = _BASE._get_json_setting(context, key, [])
        if not isinstance(pending, list) or not pending:
            return
        kept = []
        for item in pending:
            if not isinstance(item, dict):
                continue
            message = item.get("Msg")
            try:
                notification_id = int(item.get("Id") or 0)
                item_message_id = (
                    int(message.get("Id", message.get("ChatMessageId", 0)) or 0)
                    if isinstance(message, dict) else 0
                )
            except (TypeError, ValueError):
                kept.append(item)
                continue
            if notification_id in notification_ids and item_message_id == message_id:
                continue
            kept.append(item)
        _BASE._set_json_setting(context, key, kept)


async def _flush_hub_notifications(player_id: int, websocket: WebSocket, context) -> None:
    key = _pending_hub_notification_key(player_id, context)
    if key is None:
        return
    # Pop before I/O so concurrently queued notifications remain pending.
    with _NOTIFICATION_LOCK:
        pending = _BASE._get_json_setting(context, key, [])
        if not isinstance(pending, list) or not pending:
            return
        pending = [
            item
            for item in pending
            if isinstance(item, dict)
            and int(item.get("Id") or 0) not in _NON_DURABLE_HUB_NOTIFICATION_IDS
        ]
        _BASE._set_json_setting(context, key, [])
    unsent: list[dict[str, Any]] = []
    for index, item in enumerate(pending):
        if not isinstance(item, dict):
            continue
        try:
            await websocket.send_text(
                _hub_notification_frame(
                    int(item.get("Id") or 0),
                    item.get("Msg") if isinstance(item.get("Msg"), dict) else {},
                )
            )
        except Exception:
            unsent = [value for value in pending[index:] if isinstance(value, dict)]
            break
    if unsent:
        with _NOTIFICATION_LOCK:
            queued = _BASE._get_json_setting(context, key, [])
            if not isinstance(queued, list):
                queued = []
            _BASE._set_json_setting(context, key, [*unsent, *queued][-100:])


async def _send_hub_notification(
    player_id: int,
    notification_id: int,
    message: dict[str, Any],
    *,
    context=None,
    queue_if_offline: bool = True,
) -> None:
    frame = _hub_notification_frame(notification_id, message)
    if context is None:
        return
    transient = context.require_transient()
    if not await transient.route_player_online(API_VERSION, HUB_TRANSPORT, player_id):
        if queue_if_offline:
            _queue_hub_notification(player_id, notification_id, message, context)
        return
    await transient.publish_delivery(
        api_version=API_VERSION,
        transport=HUB_TRANSPORT,
        player_ids=[player_id],
        messages=[frame],
    )


def maintenance_capabilities(*, context) -> dict[str, Any]:
    connected_clients = context.require_transient().local_connection_count(
        api_version=API_VERSION,
        transport=HUB_TRANSPORT,
    )
    return {
        "snapshot_supported": True,
        "realtime_supported": True,
        "realtime_cancel_supported": False,
        "connected_clients": connected_clients,
        "transport": "signalr",
        "hub": "<NotificationService>hub/v1",
        "method": "Notification",
        "notification_id": 25,
    }


def _queue_if_not_delivered(
    player_id: int,
    notification_id: int,
    message: dict[str, Any],
    context,
    queue_if_offline: bool,
) -> None:
    # Retained as a small compatibility hook for durable message-like
    # notifications; Redis Pub/Sub itself is intentionally not a durable queue.
    if queue_if_offline:
        _queue_hub_notification(player_id, notification_id, message, context)


def timed_content_capabilities(*, context) -> dict[str, Any]:
    return {
        "weekly_supported": True,
        "weekly_schedule_key": WEEKLY_SCHEDULE_KEY,
        "weekly_end_field": "ChallengeMap.EndAt",
        "weekly_timer_unit": "absolute_utc_datetime",
        "daily_supported": True,
        "daily_schedule_key": DAILY_CHECKLIST_SCHEDULE_KEY,
        "daily_timer_field": None,
        "storefront_supported": True,
        "storefront_schedule_supported": True,
        "storefront_schedule_key": STOREFRONT_SCHEDULE_KEY,
        "storefront_timer_field": "GiftDropStorefront.NextUpdate",
        "storefront_timer_unit": "absolute_utc_datetime",
        # The dated client proves the DTO and timer field, while the catalog
        # and daily rotation are the adapter's documented restored policy.
        "storefront_catalog_source": "adapter_restored_policy",
        "realtime_refresh_supported": False,
        "refresh_behavior": "next_normal_request",
    }


def validate_moderation_action(
    *,
    action: str,
    target_type: str,
    target_id: str,
    context,
) -> None:
    if action != "quarantine":
        raise ValueError("This adapter validates only content quarantine actions.")
    if target_type == "image":
        with context.db.connection() as conn:
            asset = conn.execute(
                "SELECT 1 FROM data_assets WHERE asset_id = ?",
                (str(target_id),),
            ).fetchone()
        if asset is None:
            raise ValueError("The reported image no longer exists.")
        return
    try:
        numeric_target_id = int(target_id)
    except (TypeError, ValueError) as exc:
        raise ValueError("The reported content target ID is invalid.") from exc
    if numeric_target_id <= 0:
        raise ValueError("The reported content target ID is invalid.")
    if target_type == "room":
        if (
            _find_coach_room_by_id(numeric_target_id, context) is not None
            or _find_dorm_room_by_room_id(context, numeric_target_id) is not None
        ):
            raise ValueError(
                "Only player-created non-dorm rooms can be quarantined through a report case."
            )
        room = next(
            (
                record
                for record in _all_ugc_records(
                    context,
                    include_quarantined=True,
                )
                if int(record["version"]["room_id"]) == numeric_target_id
            ),
            None,
        )
        if room is None:
            raise ValueError("The reported player-created room no longer exists.")
        return
    if target_type == "invention":
        record = _record_for_invention(
            _invention_records(context),
            numeric_target_id,
        )
        if record is None:
            raise ValueError("The reported invention no longer exists.")
        return
    if target_type == "player_event":
        event = next(
            (
                item
                for item in _global_player_events(context)
                if int(item.get("PlayerEventId", 0) or 0) == numeric_target_id
            ),
            None,
        )
        if event is None:
            raise ValueError("The reported player event no longer exists.")
        return
    raise ValueError(
        "This client adapter can quarantine only rooms, inventions, player events, and images."
    )


async def fanout_maintenance(*, state: dict[str, Any], context) -> dict[str, Any]:
    message = {"StartsInMinutes": int(state["starts_in_minutes"])}
    frame = _hub_notification_frame(25, message)
    player_ids = await context.require_transient().route_player_ids(
        API_VERSION, HUB_TRANSPORT
    )
    await context.require_transient().publish_delivery(
        api_version=API_VERSION,
        transport=HUB_TRANSPORT,
        player_ids=player_ids,
        messages=[frame],
    )
    return {
        "delivered_clients": len(player_ids),
        "failed_deliveries": [],
    }


MAINTENANCE_EJECTION_GRACE_SECONDS = 20.0
MAINTENANCE_EJECTION_COMPLETE_SECONDS = 180.0


async def _maintenance_ejection_candidates(context) -> list[int]:
    candidates: list[int] = []
    transient = context.require_transient()
    hub_ids = await transient.route_player_ids(API_VERSION, HUB_TRANSPORT)
    presence_ids = await transient.route_player_ids(API_VERSION, PRESENCE_TRANSPORT)
    for player_id in sorted({int(value) for value in [*hub_ids, *presence_ids]}):
        player = _find_player_by_legacy_id_25april2019(context, int(player_id))
        if player is None:
            continue
        session = await transient.get_membership(player_id)
        if not isinstance(session, dict):
            session = _player_state(player).get("game_session")
        if (
            isinstance(session, dict)
            and str(session.get("Name") or "").casefold() == "dormroom"
        ):
            continue
        candidates.append(int(player_id))
    return candidates


async def enforce_maintenance_room_lock(
    *,
    context,
    player_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Eject the selected connected players while maintenance locks room travel."""
    if player_ids is None:
        transient = context.require_transient()
        hub_ids = await transient.route_player_ids(API_VERSION, HUB_TRANSPORT)
        presence_ids = await transient.route_player_ids(
            API_VERSION, PRESENCE_TRANSPORT
        )
        player_ids = sorted({int(value) for value in [*hub_ids, *presence_ids]})
    else:
        player_ids = sorted({int(player_id) for player_id in player_ids})
    ejected = 0
    already_in_dorm = 0
    detached_without_hub = 0
    presence_fallbacks = 0
    results: list[dict[str, Any]] = []
    for player_id in player_ids:
        result = await _eject_connected_player_to_dorm(
            int(player_id),
            context=context,
            is_ban=False,
            duration_seconds=0,
            message="Server maintenance has started.",
        )
        results.append({"player_id": int(player_id), **result})
        status = str(result.get("status") or "")
        if status == "ejected":
            ejected += 1
        elif status == "already_in_dorm":
            already_in_dorm += 1
        elif status == "detached_without_hub":
            detached_without_hub += 1
            await context.require_transient().publish_delivery(
                api_version=API_VERSION,
                transport=PRESENCE_TRANSPORT,
                player_ids=[player_id],
                messages=[],
                disconnect_code=4003,
                disconnect_reason="Server maintenance has started.",
            )
            presence_fallbacks += 1
    return {
        "status": "enforced",
        "connected_players": len(player_ids),
        "ejected": ejected,
        "already_in_dorm": already_in_dorm,
        "detached_without_hub": detached_without_hub,
        "presence_fallbacks": presence_fallbacks,
        "players": results,
    }


async def _maintenance_deadline_worker(
    *,
    revision: int,
    starts_at_utc: str,
    context,
) -> None:
    try:
        deadline = _parse_recnet_datetime(starts_at_utc)
        if deadline is None:
            return
        drain_start = deadline + timedelta(
            seconds=MAINTENANCE_EJECTION_GRACE_SECONDS
        )
        hard_complete = deadline + timedelta(
            seconds=MAINTENANCE_EJECTION_COMPLETE_SECONDS
        )
        scheduling_margin = min(
            5.0,
            max(
                0.0,
                (
                    MAINTENANCE_EJECTION_COMPLETE_SECONDS
                    - MAINTENANCE_EJECTION_GRACE_SECONDS
                )
                * 0.05,
            ),
        )
        drain_complete = hard_complete - timedelta(seconds=scheduling_margin)
        delay = max(
            0.0,
            (drain_start - datetime.now(timezone.utc)).total_seconds(),
        )
        if delay > 0:
            await asyncio.sleep(delay)
        while True:
            current = context.get_maintenance_state()
            if (
                not bool(current.get("active"))
                or int(current.get("revision") or 0) != revision
                or str(current.get("starts_at_utc") or "") != starts_at_utc
                or not _maintenance_room_lock_active(context)
            ):
                return
            candidates = await _maintenance_ejection_candidates(context)
            now = datetime.now(timezone.utc)
            remaining_seconds = max(
                0.0,
                (drain_complete - now).total_seconds(),
            )
            if remaining_seconds <= 0:
                if candidates:
                    await enforce_maintenance_room_lock(
                        context=context,
                        player_ids=candidates,
                    )
                hard_remaining = max(
                    0.0,
                    (hard_complete - datetime.now(timezone.utc)).total_seconds(),
                )
                if hard_remaining <= 0:
                    return
                await asyncio.sleep(min(2.0, hard_remaining))
                continue
            if not candidates:
                # Keep the worker alive for players who reconnect during the
                # drain window; the room lock already prevents onward travel.
                await asyncio.sleep(min(2.0, remaining_seconds))
                continue
            await enforce_maintenance_room_lock(
                context=context,
                player_ids=[candidates[0]],
            )
            remaining_candidates = await _maintenance_ejection_candidates(context)
            if not remaining_candidates:
                await asyncio.sleep(min(2.0, remaining_seconds))
                continue
            interval = remaining_seconds / len(remaining_candidates)
            await asyncio.sleep(max(0.05, min(interval, remaining_seconds)))
    except asyncio.CancelledError:
        raise
    finally:
        task = getattr(context, "_rr25_maintenance_deadline_task", None)
        if task is asyncio.current_task():
            setattr(context, "_rr25_maintenance_deadline_task", None)


def arm_maintenance_deadline(*, state: dict[str, Any], context) -> dict[str, Any]:
    current_task = getattr(context, "_rr25_maintenance_deadline_task", None)
    if current_task is not None and not current_task.done():
        current_task.cancel()
    setattr(context, "_rr25_maintenance_deadline_task", None)
    if not bool(state.get("active")):
        return {"status": "disarmed"}
    starts_at_utc = str(state.get("starts_at_utc") or "")
    deadline = _parse_recnet_datetime(starts_at_utc)
    if deadline is None:
        return {"status": "invalid_deadline"}
    task = asyncio.get_running_loop().create_task(
        _maintenance_deadline_worker(
            revision=int(state.get("revision") or 0),
            starts_at_utc=starts_at_utc,
            context=context,
        )
    )
    setattr(context, "_rr25_maintenance_deadline_task", task)
    return {
        "status": "armed",
        "starts_at_utc": starts_at_utc,
        "revision": int(state.get("revision") or 0),
    }


async def stop_maintenance_deadline(*, context) -> None:
    task = getattr(context, "_rr25_maintenance_deadline_task", None)
    setattr(context, "_rr25_maintenance_deadline_task", None)
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _broadcast_presence_update(player_id: int, context) -> None:
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        return
    state = _player_state(player)
    presence = await _presence_payload(player_id, state, context)
    game_session = presence.get("GameSession")
    recipient_ids = await context.require_transient().route_player_ids(
        API_VERSION, HUB_TRANSPORT
    )
    for recipient_value in recipient_ids:
        recipient_id = int(recipient_value)
        await _send_hub_notification(recipient_id, 12, presence, context=context)
        if isinstance(game_session, dict):
            await _send_hub_notification(recipient_id, 13, game_session, context=context)


async def _broadcast_profile_update(player_id: int, context) -> None:
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        return
    profile = _serialize_profile_25april2019(player)
    recipient_ids = await context.require_transient().route_player_ids(
        API_VERSION, HUB_TRANSPORT
    )
    for recipient_value in recipient_ids:
        await _send_hub_notification(
            int(recipient_value), 11, profile, context=context
        )


async def _broadcast_invention_cache_invalidation(context) -> None:
    # Push ID 100 invalidates invention caches and needs no DTO payload.
    recipient_ids = await context.require_transient().route_player_ids(
        API_VERSION, HUB_TRANSPORT
    )
    for recipient_value in recipient_ids:
        await _send_hub_notification(
            int(recipient_value), 100, {}, context=context
        )


def _schedule_presence_update(player_id: int, context) -> None:
    if player_id <= 0:
        return
    try:
        asyncio.get_running_loop().create_task(_broadcast_presence_update(player_id, context))
    except RuntimeError:
        pass


async def _mark_presence_heartbeat(
    player_id: int,
    context,
    *,
    online: bool,
    connection_id: str | None = None,
    transport: str = "http-presence",
) -> dict[str, Any] | None:
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        return None
    state = _player_state(player)
    transient = context.require_transient()
    was_online = await transient.player_online(player_id)
    presence = await _presence_payload(player_id, state, context)
    presence["IsOnline"] = online
    if online and connection_id:
        await transient.refresh_connection(
            connection_id=connection_id,
            api_version=API_VERSION,
            transport=transport,
            player_id=player_id,
            presence=presence,
        )
    else:
        await transient.update_http_presence(
            api_version=API_VERSION,
            player_id=player_id,
            presence=presence,
            online=online,
        )
    if was_online != online:
        _schedule_presence_update(player_id, context)
    return player


async def _presence_for_heartbeat(player_id: int, requested_session_id: int, context) -> tuple[str, dict[str, Any]]:
    player = _find_player_by_legacy_id_25april2019(context, player_id)
    if player is None:
        return "PlayerNotFound", {}
    state = _player_state(player)
    authoritative = await _authoritative_game_session_for_player(player_id, state, context)
    if requested_session_id <= 0:
        state = dict(state)
        state.pop("game_session", None)
        await context.require_transient().set_membership(player_id, None)
        return "", await _presence_payload(player_id, state, context)

    if authoritative is not None:
        authoritative_id = int(authoritative.get("GameSessionId") or 0)
        await context.require_transient().set_membership(player_id, authoritative)
        # Return current presence when a heartbeat carries a stale session ID.
        if authoritative_id > 0:
            return "", await _presence_payload(player_id, state, context)

    with context.db.connection() as conn:
        instances = _read_game_instances(conn)
    instance = next(
        (
            item
            for item in instances
            if int(item.get("GameSessionId") or 0) == requested_session_id
        ),
        None,
    )
    if instance is None:
        return "GameSessionNotFound", await _presence_payload(player_id, state, context)
    active = await _repair_player_game_session(player, requested_session_id, context)
    if active is None:
        # The repair helper only accepts existing membership, the player's
        # durable active session, or a server-recorded invitation.
        return "PlayerNotInGameSession", await _presence_payload(player_id, state, context)
    await context.require_transient().set_membership(player_id, active)
    return "", await _presence_payload(player_id, _player_state(player), context)


async def handle_websocket(*, websocket: WebSocket, route_path: str, context) -> None:
    path = _clean_route_path(route_path).casefold()
    if path not in {"api/presence/v3/heartbeatwebsocket", "hub/v1"}:
        await websocket.close(code=4404, reason="Unknown endpoint.")
        return

    if path == "hub/v1":
        row = context.player_from_request(websocket, API_VERSION)
        player_id = 0
        if row is not None:
            try:
                row_state = json.loads(row["state_json"] or "{}")
            except Exception:
                row_state = {}
            player_id = int(row_state.get("legacy_player_id") or row_state.get("recnet_id") or 0)
        if player_id <= 0:
            negotiated_player = await _player_from_hub_connection_id(
                context, str(websocket.query_params.get("id") or "")
            )
            if negotiated_player is not None:
                player_id = _legacy_id_for_player(negotiated_player)

        if player_id <= 0:
            await websocket.close(code=4401, reason="Authenticated negotiation is required.")
            return
        hub_player = _find_player_by_legacy_id_25april2019(context, player_id)
        if hub_player is None:
            await websocket.close(code=4401, reason="Authenticated player is required.")
            return
        context.assert_player_not_banned(hub_player["player_id"])
        await websocket.accept()
        hub_connection_id = await context.require_transient().register_connection(
            api_version=API_VERSION,
            transport=HUB_TRANSPORT,
            player_id=player_id,
            websocket=websocket,
        )
                        # Mark online during the gap before the first heartbeat.
        await _mark_presence_heartbeat(
            player_id,
            context,
            online=True,
            connection_id=hub_connection_id,
            transport=HUB_TRANSPORT,
        )
        handshake_complete = False
        try:
            while True:
                message = await websocket.receive()
                await context.require_transient().refresh_connection(
                    connection_id=hub_connection_id,
                    api_version=API_VERSION,
                    transport=HUB_TRANSPORT,
                    player_id=player_id,
                )
                if message.get("type") == "websocket.disconnect":
                    break
                data = message.get("text") or (message.get("bytes") or b"").decode("utf-8", errors="ignore")
                if "protocol" in data and not handshake_complete:
                    handshake_complete = True
                    await websocket.send_text("{}\x1e")
                    if player_id > 0:
                        await _flush_hub_notifications(player_id, websocket, context)
                        # Seed the client's confirmed presence subscription.
                    player = (
                        _find_player_by_legacy_id_25april2019(context, player_id)
                        if player_id > 0
                        else None
                    )
                    if player is not None:
                        if _maintenance_room_lock_active(context):
                            maintenance_state = context.get_maintenance_state()
                            maintenance_deadline = _parse_recnet_datetime(
                                maintenance_state.get("starts_at_utc")
                            )
                            if (
                                maintenance_deadline is not None
                                and datetime.now(timezone.utc)
                                >= maintenance_deadline
                                + timedelta(
                                    seconds=MAINTENANCE_EJECTION_COMPLETE_SECONDS
                                )
                            ):
                                await _eject_connected_player_to_dorm(
                                    player_id,
                                    context=context,
                                    is_ban=False,
                                    duration_seconds=0,
                                    message="Server maintenance has started.",
                                )
                        # Rehydrate relationship state on hub connection.
                        relationships = _load_relationships(player, context)
                        for relationship in relationships:
                            await websocket.send_text(
                                _hub_notification_frame(1, _relationship_dto(relationship))
                            )
                        # Seed related players' presence before the profile UI evaluates actions.
                        related_player_ids = sorted(
                            {
                                int(item.get("PlayerID") or 0)
                                for item in relationships
                                if int(item.get("PlayerID") or 0) > 0
                            }
                        )
                        for related_player_id in related_player_ids:
                            related_player = _find_player_by_legacy_id_25april2019(
                                context, related_player_id
                            )
                            if related_player is None:
                                continue
                            related_presence = await _presence_payload(
                                related_player_id,
                                _player_state(related_player),
                                context,
                            )
                            await websocket.send_text(
                                _hub_notification_frame(12, related_presence)
                            )
                            related_session = related_presence.get("GameSession")
                            if isinstance(related_session, dict):
                                await websocket.send_text(
                                    _hub_notification_frame(13, related_session)
                                )
                        # Ensure each pending request has one visible FriendInvite.
                        message_key = _canonical_player_setting_key(
                            "messages", player["player_id"]
                        )
                        stored_messages = _BASE._get_json_setting(
                            context, message_key, []
                        )
                        if not isinstance(stored_messages, list):
                            stored_messages = []
                        visible_request_senders = {
                            int(item.get("FromPlayerId") or 0)
                            for item in stored_messages
                            if isinstance(item, dict)
                            and int(item.get("Type") or -1) == 4
                        }
                        for relationship in relationships:
                            sender_id = int(relationship.get("PlayerID") or 0)
                            if (
                                int(relationship.get("RelationshipType") or 0) == 2
                                and sender_id > 0
                                and sender_id not in visible_request_senders
                            ):
                                await _create_recnet_message(
                                    player,
                                    from_player_id=sender_id,
                                    message_type=4,
                                    context=context,
                                )
                        notification = json.dumps(
                            {
                                "Id": "12",
                                "Msg": await _presence_payload(
                                    player_id, _player_state(player), context
                                ),
                            },
                            separators=(",", ":"),
                        )
                        invocation = {
                            "type": 1,
                            "target": "Notification",
                            "arguments": [notification],
                        }
                        await websocket.send_text(
                            json.dumps(invocation, separators=(",", ":")) + "\x1e"
                        )
        except WebSocketDisconnect:
            pass
        except Exception as e:
            print(f"WebSocket Exception in hub/v1: {e}")
        finally:
            if player_id > 0:
                still_online = await context.require_transient().unregister_connection(
                    hub_connection_id
                )
                if not still_online:
                    _schedule_presence_update(player_id, context)
        return

    # Prefer bearer authentication; retain the query token compatibility path.
    row = context.player_from_request(websocket, API_VERSION)
    player_id = 0
    token = str(websocket.query_params.get("loginLockToken") or "")
    if row is not None:
        try:
            row_state = json.loads(row["state_json"] or "{}")
        except Exception:
            row_state = {}
        player_id = int(row_state.get("legacy_player_id") or row_state.get("recnet_id") or 0)
        if token:
            await _remember_presence_login_lock_token(row, token, context)
    else:
        token_player = await _player_from_presence_login_lock_token(context, token)
        if token_player is not None:
            player_id = _legacy_id_for_player(token_player)

    player = _find_player_by_legacy_id_25april2019(context, player_id) if player_id > 0 else None
    if player is None:
        await websocket.close(code=4001, reason="Player not found.")
        return

    await websocket.accept()
    presence_connection_id = await context.require_transient().register_connection(
        api_version=API_VERSION,
        transport=PRESENCE_TRANSPORT,
        player_id=player_id,
        websocket=websocket,
    )
    try:
        while True:
            request_text = await websocket.receive_text()
            try:
                heartbeat_request = json.loads(request_text)
            except (TypeError, ValueError, json.JSONDecodeError):
                heartbeat_request = {}
            requested_session_id = int(
                heartbeat_request.get("GameSessionId", heartbeat_request.get("gameSessionId", 0)) or 0
            ) if isinstance(heartbeat_request, dict) else 0
            if await _mark_presence_heartbeat(
                player_id,
                context,
                online=True,
                connection_id=presence_connection_id,
                transport=PRESENCE_TRANSPORT,
            ) is None:
                await websocket.close(code=4001, reason="Player not found.")
                return
            error, presence = await _presence_for_heartbeat(player_id, requested_session_id, context)
            await websocket.send_text(
                json.dumps(
                    {"Error": error, "Presence": presence},
                    separators=(",", ":"),
                )
            )
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        still_online = await context.require_transient().unregister_connection(
            presence_connection_id
        )
        if not still_online:
            _schedule_presence_update(player_id, context)
