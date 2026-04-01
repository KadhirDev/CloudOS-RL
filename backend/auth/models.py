"""
Auth Models
============
Simple in-memory user store. In production, replace with DB lookup.
Roles (lowest → highest privilege):
  viewer   — read-only dashboard
  user     — can submit workloads
  engineer — full engineering view + SHAP
  admin    — all access + operational controls
  executive— KPI/summary view only (no engineering controls)
"""

from enum import Enum
from typing import Dict, Optional
from pydantic import BaseModel


class Role(str, Enum):
    viewer    = "viewer"
    user      = "user"
    engineer  = "engineer"
    admin     = "admin"
    executive = "executive"


# Role privilege ordering — higher index = more privileged (except executive is lateral)
ROLE_CAN_SCHEDULE = {Role.user, Role.engineer, Role.admin}
ROLE_CAN_ADMIN    = {Role.admin}
ROLE_READONLY     = {Role.viewer, Role.executive}


class TokenPayload(BaseModel):
    sub:  str
    role: Role


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    role:         Role
    username:     str


# ── In-memory user store — replace with DB in production ─────────────────────
# Format: username → { "password": plaintext_for_dev, "role": Role }
# IMPORTANT: use hashed passwords in production
_USERS: Dict[str, Dict] = {
    "viewer":    {"password": "viewer123",    "role": Role.viewer},
    "alice":     {"password": "alice123",     "role": Role.user},
    "engineer":  {"password": "eng123",       "role": Role.engineer},
    "admin":     {"password": "admin123",     "role": Role.admin},
    "executive": {"password": "exec123",      "role": Role.executive},
    "cto":       {"password": "cto123",       "role": Role.executive},
}


def get_user(username: str) -> Optional[Dict]:
    return _USERS.get(username)