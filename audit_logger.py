import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import config

logger = logging.getLogger(__name__)


SENSITIVE_KEYS = {
    "API_HASH",
    "BOT_TOKEN",
    "SESSION_STRING",
    "MONGO_URI",
}

PLACEHOLDER_PREFIXES = ("your_", "YOUR_")
PLACEHOLDER_EXACT = {
    "0",
    "none",
    "null",
    "changeme",
    "replace_me",
    "your_api_id_here",
    "your_api_hash_here",
    "your_bot_token",
    "your_session_string_here",
}


def mask_value(key: str, value: Any) -> str:
    if value is None:
        return "[EMPTY]"
    raw = str(value)
    if not raw:
        return "[EMPTY]"
    if key in SENSITIVE_KEYS:
        if len(raw) <= 8:
            return "***"
        return f"{raw[:3]}***{raw[-3:]}"
    return raw


def _looks_unset(raw: Any) -> bool:
    if raw is None:
        return True
    if isinstance(raw, (int, float)):
        return int(raw) == 0

    text = str(raw).strip()
    if not text:
        return True

    lower = text.lower()
    if lower in PLACEHOLDER_EXACT:
        return True

    for prefix in PLACEHOLDER_PREFIXES:
        if text.startswith(prefix):
            return True

    return False


def find_missing_keys(env: Mapping[str, Any], required_keys: Iterable[str]) -> list[str]:
    missing: list[str] = []
    for key in required_keys:
        if _looks_unset(env.get(key)):
            missing.append(key)
    return missing


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


async def send_log(client, text: str, *, disable_preview: bool = True) -> bool:
    """Send a log message to LOG_CHANNEL_ID if configured."""
    log_chat_id = getattr(config, "LOG_CHANNEL_ID", 0) or 0
    if not log_chat_id:
        return False

    try:
        await client.send_message(
            log_chat_id,
            text,
            disable_web_page_preview=disable_preview,
        )
        return True
    except Exception as exc:
        logger.warning("Failed to send log message: %s", exc)
        return False


def mention_user(user) -> str:
    if not user:
        return "Unknown"
    name = (getattr(user, "first_name", "") or "").strip() or "User"
    user_id = getattr(user, "id", 0) or 0
    if user_id:
        return f"<a href='tg://user?id={user_id}'>{name}</a> (<code>{user_id}</code>)"
    return name


def format_startup_report(component: str, missing_keys: list[str]) -> str:
    title = f"🟢 <b>{component} started</b>"
    if not missing_keys:
        return f"{title}\n<b>Time:</b> {utc_now()}\n✅ Config: all required keys present"

    missing_lines = "\n".join(f"• <code>{k}</code>" for k in missing_keys)
    return (
        f"{title}\n<b>Time:</b> {utc_now()}\n"
        f"⚠️ <b>Missing/placeholder config keys:</b>\n{missing_lines}"
    )


def format_download_log(user, title: str, *, season: str = "", quality: str = "", language: str = "", size: str = "") -> str:
    parts = [
        "⬇️ <b>Download served</b>",
        f"<b>User:</b> {mention_user(user)}",
        f"<b>Title:</b> {title}",
    ]
    if season:
        parts.append(f"<b>Season:</b> {season}")
    if quality:
        parts.append(f"<b>Quality:</b> {quality}")
    if language:
        parts.append(f"<b>Language:</b> {language}")
    if size:
        parts.append(f"<b>Size:</b> {size}")
    parts.append(f"<b>Time:</b> {utc_now()}")
    return "\n".join(parts)


def format_env_change(actor, key: str, old_value: Any, new_value: Any) -> str:
    return (
        "🛠 <b>.env updated</b>\n"
        f"<b>By:</b> {mention_user(actor)}\n"
        f"<b>Key:</b> <code>{key}</code>\n"
        f"<b>Old:</b> <code>{mask_value(key, old_value)}</code>\n"
        f"<b>New:</b> <code>{mask_value(key, new_value)}</code>\n"
        f"<b>Time:</b> {utc_now()}"
    )
