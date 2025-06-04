from fastapi import APIRouter

from app.api.routes import sector_router
from app.core.config import settings

api_router = APIRouter()
api_router.include_router(items.router)


if settings.ENVIRONMENT == "local":
    api_router.include_router(sector_router.router)
