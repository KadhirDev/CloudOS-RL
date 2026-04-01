"""
Auth Router
============
POST /auth/login  → returns JWT token
GET  /auth/me     → returns current user info (requires valid token)
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, status
from backend.auth.models import LoginRequest, TokenResponse, Role, get_user
from backend.auth.security import create_token, require_auth, TokenPayload

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest) -> TokenResponse:
    user = get_user(request.username)
    if user is None or user["password"] != request.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
        )
    role  = user["role"]
    token = create_token(request.username, role)
    logger.info("Auth: user '%s' logged in with role '%s'", request.username, role.value)
    return TokenResponse(
        access_token=token,
        role=role,
        username=request.username,
    )


@router.get("/me")
async def me(user: TokenPayload = Depends(require_auth)) -> dict:
    return {"username": user.sub, "role": user.role.value}


@router.post("/logout")
async def logout() -> dict:
    # JWT is stateless — client discards token
    return {"message": "Logged out. Discard your token."}