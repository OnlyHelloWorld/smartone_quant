import sentry_sdk
from fastapi import FastAPI
from fastapi.routing import APIRoute
from starlette.middleware.cors import CORSMiddleware


def custom_generate_unique_id(route: APIRoute) -> str:
    """
    参数:
    - route (APIRoute): FastAPI 路由对象。
    通过路由的标签和名称生成唯一ID。
    """
    return f"{route.tags[0]}-{route.name}"

if settings.SENTRY_DSN and settings.ENVIRONMENT != "local":
    """
    初始化 Sentry SDK 以捕获错误和性能数据。
    仅在非本地环境中启用。
    """
    sentry_sdk.init(dsn=str(settings.SENTRY_DSN), enable_tracing=True)
