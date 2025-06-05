from collections.abc import Generator
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt.exceptions import InvalidTokenError
from pydantic import ValidationError
from sqlmodel import Session

from app.core import security
from app.core.config import settings
from app.core.db import engine
from app.models import TokenPayload, User

# OAuth2PasswordBearer 是 FastAPI 提供的用于 OAuth2 认证流程的工具，
# 这里指定了获取 token 的接口地址
reusable_oauth2 = OAuth2PasswordBearer(
    tokenUrl=f"{settings.API_V1_STR}/login/access-token"
)

# 获取数据库会话的依赖项，使用 yield 生成器模式，确保用完后自动关闭
# Generator[Session, None, None] 表示生成 Session 类型对象
def get_db() -> Generator[Session, None, None]:
    """
    获取数据库会话的依赖项。
    :return: 返回一个生成器，生成 SQLModel 的 Session 对象。
    """
    with Session(engine) as session:
        yield session

# SessionDep 用于类型注解，表示依赖于 get_db 的 Session
SessionDep = Annotated[Session, Depends(get_db)]
# TokenDep 用于类型注解，表示依赖于 reusable_oauth2 的 token 字符串
TokenDep = Annotated[str, Depends(reusable_oauth2)]

# 获取当前用户的依赖项，通过解析 token 并从数据库获取用户对象
# session: 数据库会话，token: 前端传递的 JWT token
def get_current_user(session: SessionDep, token: TokenDep) -> User:
    """
    获取当前用户对象。
    :param session: 数据库会话对象。
    :param token: JWT token 字符串。
    :return: User 对象，如果 token 无效或用户不存在则抛出异常。
    """
    try:
        # 解码 JWT token，校验签名和算法
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[security.ALGORITHM]
        )
        # 将解码后的 payload 转换为 TokenPayload 对象
        token_data = TokenPayload(**payload)
    except (InvalidTokenError, ValidationError):
        # token 无效或 payload 校验失败，抛出 403 异常
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials",
        )
    # 根据 token 中的 sub 字段（用户ID）查询用户
    user = session.get(User, token_data.sub)
    if not user:
        # 用户不存在，抛出 404 异常
        raise HTTPException(status_code=404, detail="User not found")
    if not user.is_active:
        # 用户未激活，抛出 400 异常
        raise HTTPException(status_code=400, detail="Inactive user")
    return user

# CurrentUser 用于类型注解，表示依赖于 get_current_user 的 User 对象
CurrentUser = Annotated[User, Depends(get_current_user)]

# 获取当前活跃超级用户的依赖项
# current_user: 当前用户对象
def get_current_active_superuser(current_user: CurrentUser) -> User:
    """
    获取当前活跃的超级用户。
    :param current_user: 当前用户对象。
    :return: User 对象，如果不是超级用户则抛出 403 异常。
    """
    if not current_user.is_superuser:
        # 如果不是超级用户，抛出 403 异常
        raise HTTPException(
            status_code=403, detail="The user doesn't have enough privileges"
        )
    return current_user
