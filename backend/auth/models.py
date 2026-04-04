"""
Auth Models
============
User store with two layers:
  1. DEMO_USERS dict  — hardcoded, never mutated, always available
  2. data/users.json  — registered users, persisted to disk

Login checks registered users first, then falls back to demo accounts.
Passwords for registered users are bcrypt-hashed.
Demo account passwords remain plaintext (dev only).

Roles:
  viewer | user | engineer | admin | executive
  All new signups get role = user.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from enum import Enum
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── bcrypt via passlib ────────────────────────────────────────────────────────
try:
    from passlib.context import CryptContext

    _pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    _BCRYPT_OK = True
except ImportError:
    _pwd_context = None
    _BCRYPT_OK = False
    logger.warning(
        "passlib[bcrypt] not installed — registered user passwords will use "
        "plain comparison fallback. Install with: pip install passlib[bcrypt]"
    )


# ── Password helpers ──────────────────────────────────────────────────────────
def _safe_bcrypt_input(plain: str) -> str:
    """
    bcrypt supports a maximum of 72 bytes.
    Truncate safely at the byte level to avoid runtime errors.
    """
    return plain.encode("utf-8")[:72].decode("utf-8", "ignore")


def hash_password(plain: str) -> str:
    """
    Hash password for persisted registered users.
    Uses bcrypt when available, otherwise falls back to plain: prefix for dev use.
    """
    if _BCRYPT_OK and _pwd_context is not None:
        safe = _safe_bcrypt_input(plain)
        return _pwd_context.hash(safe)

    # Dev fallback only — never use in production
    return f"plain:{plain}"


def verify_password(plain: str, stored: str) -> bool:
    """
    Verify a plain password against stored value.

    Supports:
      - bcrypt hashes (registered users)
      - plain:<password> fallback
      - raw plaintext demo passwords
    """
    if stored.startswith("plain:"):
        return plain == stored[6:]

    if _BCRYPT_OK and _pwd_context is not None:
        try:
            safe = _safe_bcrypt_input(plain)
            return _pwd_context.verify(safe, stored)
        except Exception:
            # Stored value may be plaintext demo password, not a bcrypt hash
            return plain == stored

    return plain == stored


# ── Role definitions ──────────────────────────────────────────────────────────
class Role(str, Enum):
    viewer = "viewer"
    user = "user"
    engineer = "engineer"
    admin = "admin"
    executive = "executive"


ROLE_CAN_SCHEDULE = {Role.user, Role.engineer, Role.admin}
ROLE_CAN_ADMIN = {Role.admin}
ROLE_READONLY = {Role.viewer, Role.executive}

DEFAULT_ROLE = Role.user


# ── Pydantic schemas ──────────────────────────────────────────────────────────
class TokenPayload(BaseModel):
    sub: str
    role: Role


class LoginRequest(BaseModel):
    username: str
    password: str


class RegisterRequest(BaseModel):
    username: str
    password: str
    confirm_password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: Role
    username: str


class RegisterResponse(BaseModel):
    message: str
    username: str
    role: Role


# ── Demo accounts (hardcoded, never written to disk) ─────────────────────────
# Format: username → { "password": str (plaintext), "role": Role, "hashed": bool }
_DEMO_USERS: Dict[str, Dict] = {
    "viewer": {"password": "viewer123", "role": Role.viewer, "hashed": False},
    "alice": {"password": "alice123", "role": Role.user, "hashed": False},
    "engineer": {"password": "eng123", "role": Role.engineer, "hashed": False},
    "admin": {"password": "admin123", "role": Role.admin, "hashed": False},
    "executive": {"password": "exec123", "role": Role.executive, "hashed": False},
    "cto": {"password": "cto123", "role": Role.executive, "hashed": False},
}

# ── Registered user store (JSON-backed) ──────────────────────────────────────
_USERS_FILE = Path(os.environ.get("CLOUDOS_USERS_FILE", "data/users.json"))
_store_lock = threading.Lock()


def _load_registered() -> Dict[str, Dict]:
    """
    Load registered users from disk.

    Returns:
        Dict[str, Dict]: username → {"password": <hashed>, "role": <role_str>}
    """
    try:
        if _USERS_FILE.exists():
            with _USERS_FILE.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, dict):
                    return data
                logger.warning(
                    "UserStore: invalid JSON root in %s; expected object",
                    _USERS_FILE,
                )
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "UserStore: failed to load %s (%s) — starting empty",
            _USERS_FILE,
            exc,
        )

    return {}


def _save_registered(data: Dict[str, Dict]) -> None:
    """
    Atomically write registered user data to disk.
    """
    _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _USERS_FILE.with_suffix(".tmp.json")

    try:
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(_USERS_FILE)
    except OSError as exc:
        logger.error("UserStore: failed to save %s: %s", _USERS_FILE, exc)
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        raise


# ── Public API ────────────────────────────────────────────────────────────────
def get_user(username: str) -> Optional[Dict]:
    """
    Unified user lookup.

    Registered users take precedence over demo accounts of the same name.

    Returns:
        {
            "password": str,
            "role": Role,
            "hashed": bool
        }
        or None if not found.
    """
    username = username.strip().lower()
    if not username:
        return None

    with _store_lock:
        registered = _load_registered()

    if username in registered:
        entry = registered[username]
        try:
            role = Role(entry["role"])
        except (KeyError, ValueError):
            logger.warning(
                "UserStore: invalid role for user '%s'; skipping entry",
                username,
            )
            return None

        return {
            "password": entry["password"],
            "role": role,
            "hashed": True,
        }

    return _DEMO_USERS.get(username)


def authenticate_user(username: str, plain_password: str) -> Optional[Dict]:
    """
    Authenticate against registered users or demo accounts.

    Returns:
        user dict from get_user(...) if credentials are valid, else None.
    """
    user = get_user(username)
    if not user:
        return None

    stored_password = user["password"]
    if verify_password(plain_password, stored_password):
        return user

    return None


def register_user(username: str, plain_password: str) -> Role:
    """
    Register a new user with default role = user.

    Raises:
        ValueError: on invalid input or duplicate/reserved username
    """
    username = username.strip().lower()

    if not username:
        raise ValueError("Username cannot be empty.")
    if len(username) < 3:
        raise ValueError("Username must be at least 3 characters.")
    if len(username) > 32:
        raise ValueError("Username must be 32 characters or fewer.")
    if not username.replace("_", "").replace("-", "").isalnum():
        raise ValueError(
            "Username may only contain letters, numbers, hyphens, and underscores."
        )
    if len(plain_password) < 6:
        raise ValueError("Password must be at least 6 characters.")

    with _store_lock:
        registered = _load_registered()

        if username in registered:
            raise ValueError(f"Username '{username}' is already taken.")
        if username in _DEMO_USERS:
            raise ValueError(f"Username '{username}' is reserved.")

        registered[username] = {
            "password": hash_password(plain_password),
            "role": DEFAULT_ROLE.value,
        }
        _save_registered(registered)

    logger.info(
        "UserStore: registered new user '%s' with role '%s'",
        username,
        DEFAULT_ROLE.value,
    )
    return DEFAULT_ROLE


def list_demo_users() -> Dict[str, Dict]:
    """
    Return a copy of demo users without exposing mutable internal state.
    """
    return {
        username: {
            "password": user["password"],
            "role": user["role"],
            "hashed": user["hashed"],
        }
        for username, user in _DEMO_USERS.items()
    }