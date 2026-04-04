"""
Auth Router
============
POST /auth/login     — returns JWT token
POST /auth/register  — registers new user
GET  /auth/me        — returns current user info
POST /auth/logout    — stateless logout
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, status

from backend.auth.models import (
    LoginRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
    get_user,
    register_user,
    verify_password,
)
from backend.auth.security import create_token, require_auth, TokenPayload

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])


# ─────────────────────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest) -> TokenResponse:
    username = request.username.strip().lower()
    user = get_user(username)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
        )

    # Supports both:
    # - bcrypt hashed passwords (registered users)
    # - plaintext (demo users)
    if not verify_password(request.password, user["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
        )

    role = user["role"]
    token = create_token(username, role)

    logger.info("Auth: user '%s' logged in (role=%s)", username, role.value)

    return TokenResponse(
        access_token=token,
        role=role,
        username=username,
    )


# ─────────────────────────────────────────────────────────────────────────────
# REGISTER (NEW)
# ─────────────────────────────────────────────────────────────────────────────
@router.post(
    "/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register(request: RegisterRequest) -> RegisterResponse:
    """
    Registers a new user.

    - Password is hashed (bcrypt if available)
    - Username must be unique
    - Default role = user
    - Does NOT auto-login (safe design)
    """

    username = request.username.strip().lower()

    if request.password != request.confirm_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Passwords do not match.",
        )

    try:
        role = register_user(
            username=username,
            plain_password=request.password,
        )

    except ValueError as exc:
        detail = str(exc)

        # Smart error mapping
        if "already taken" in detail or "reserved" in detail:
            code = status.HTTP_409_CONFLICT
        else:
            code = status.HTTP_400_BAD_REQUEST

        raise HTTPException(status_code=code, detail=detail)

    except OSError as exc:
        logger.error("Auth: failed to persist user: %s", exc)

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server error while creating account. Please try again.",
        )

    logger.info("Auth: new user registered '%s' (role=%s)", username, role.value)

    return RegisterResponse(
        message=f"Account created successfully. You can now sign in as '{username}'.",
        username=username,
        role=role,
    )


# ─────────────────────────────────────────────────────────────────────────────
# CURRENT USER
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/me")
async def me(user: TokenPayload = Depends(require_auth)) -> dict:
    return {
        "username": user.sub,
        "role": user.role.value,
    }


# ─────────────────────────────────────────────────────────────────────────────
# LOGOUT
# ─────────────────────────────────────────────────────────────────────────────
@router.post("/logout")
async def logout() -> dict:
    """
    Stateless logout.
    Client must discard JWT token.
    """
    return {"message": "Logged out. Discard your token."}