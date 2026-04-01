"""
JWT Security
=============
Issues and validates JWT tokens.
Uses python-jose. Install: pip install python-jose[cryptography] passlib[bcrypt]
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from backend.auth.models import Role, TokenPayload, get_user

try:
    from jose import JWTError, jwt
except ImportError:
    raise ImportError("pip install python-jose[cryptography]")

# ── Config — override via env vars ────────────────────────────────────────────
SECRET_KEY       = os.environ.get("CLOUDOS_JWT_SECRET", "cloudos-dev-secret-change-in-production")
ALGORITHM        = "HS256"
TOKEN_EXPIRE_MIN = int(os.environ.get("CLOUDOS_JWT_EXPIRE_MIN", "480"))   # 8 hours

_bearer = HTTPBearer(auto_error=False)


# ── Token generation ──────────────────────────────────────────────────────────

def create_token(username: str, role: Role) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRE_MIN)
    payload = {
        "sub":  username,
        "role": role.value,
        "exp":  expire,
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


# ── Token validation ──────────────────────────────────────────────────────────

def _decode_token(token: str) -> TokenPayload:
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return TokenPayload(sub=data["sub"], role=Role(data["role"]))
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid or expired token: {exc}",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> Optional[TokenPayload]:
    """
    Returns TokenPayload if a valid Bearer token is present.
    Returns None if no token provided (allows optional auth).
    Raises 401 if token present but invalid.
    """
    if credentials is None:
        return None
    return _decode_token(credentials.credentials)


def require_auth(
    user: Optional[TokenPayload] = Depends(get_current_user),
) -> TokenPayload:
    """Raises 401 if not authenticated."""
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def require_role(*allowed_roles: Role):
    """
    Dependency factory. Usage:
        @router.post("/admin-action")
        async def admin_action(user=Depends(require_role(Role.admin))):
    """
    def _check(user: TokenPayload = Depends(require_auth)) -> TokenPayload:
        if user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{user.role}' is not permitted for this action. "
                       f"Required: {[r.value for r in allowed_roles]}",
            )
        return user
    return _check


def can_schedule(user: Optional[TokenPayload] = Depends(get_current_user)) -> Optional[TokenPayload]:
    """
    Scheduling permission check.
    AUTH_REQUIRED env var gates whether auth is enforced (default: False for backward compat).
    """
    import os
    if not os.environ.get("CLOUDOS_AUTH_REQUIRED", "false").lower() == "true":
        return user   # auth optional — backward compatible with existing clients

    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required to schedule workloads.")
    from backend.auth.models import ROLE_CAN_SCHEDULE
    if user.role not in ROLE_CAN_SCHEDULE:
        raise HTTPException(
            status_code=403,
            detail=f"Role '{user.role}' cannot submit workloads.",
        )
    return user