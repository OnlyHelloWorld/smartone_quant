import secrets
import warnings
import os
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    AnyUrl,
    BeforeValidator,
    EmailStr,
    computed_field,
    model_validator,
    Field,
)
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self


def parse_cors(v: Any) -> list[str] | str:
    """解析 CORS 配置"""
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",")]
    elif isinstance(v, (list, str)):
        return v
    raise ValueError(f"Invalid CORS value: {v}")


class Settings(BaseSettings):
    """应用程序配置类"""

    model_config = SettingsConfigDict(
        # 指定多个可能的 .env 文件位置
        env_file=[
            ".env",  # 当前目录
            "app/.env",  # app 目录
            "../.env",  # 上级目录
            str(Path(__file__).parent.parent / ".env"),  # app/.env
            str(Path(__file__).parent.parent.parent / ".env"),  # 项目根目录/.env
        ],
        env_ignore_empty=True,
        extra="ignore",
        case_sensitive=True,
    )

    # =============================================================================
    # 基础配置 - 必填字段改为可选并提供默认值
    # =============================================================================
    PROJECT_NAME: str = Field(default="smartone_quant", description="项目名称")
    ENVIRONMENT: Literal["local", "staging", "production"] = Field(
        default="local", description="环境类型"
    )
    API_V1_STR: str = Field(default="/api/v1", description="API 路径前缀")

    # =============================================================================
    # 安全配置
    # =============================================================================
    SECRET_KEY: str = Field(
        default_factory=lambda: secrets.token_urlsafe(32),
        description="应用密钥"
    )
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(
        default=60 * 24 * 8,  # 8 days
        description="访问令牌过期时间（分钟）"
    )

    # =============================================================================
    # CORS 配置
    # =============================================================================
    FRONTEND_HOST: str = Field(
        default="http://localhost:5173",
        description="前端主机地址"
    )
    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str, BeforeValidator(parse_cors)
    ] = Field(default=[], description="CORS 允许的源地址")

    @computed_field
    @property
    def all_cors_origins(self) -> list[str]:
        """获取所有 CORS 允许的源地址"""
        origins = [str(origin).rstrip("/") for origin in self.BACKEND_CORS_ORIGINS]
        if self.FRONTEND_HOST not in origins:
            origins.append(self.FRONTEND_HOST)
        return origins

    # =============================================================================
    # PostgreSQL 数据库配置 - 改为可选并提供默认值
    # =============================================================================
    POSTGRES_SERVER: str = Field(default="localhost", description="PostgreSQL 服务器地址")
    POSTGRES_PORT: int = Field(default=5432, description="PostgreSQL 端口")
    POSTGRES_USER: str = Field(default="postgres", description="PostgreSQL 用户名")
    POSTGRES_PASSWORD: str = Field(default="changethis", description="PostgreSQL 密码")
    POSTGRES_DB: str = Field(default="smartone_quant", description="PostgreSQL 数据库名")

    @computed_field
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        """构建 PostgreSQL 数据库连接 URI"""
        return str(MultiHostUrl.build(
            scheme="postgresql+psycopg",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_SERVER,
            port=self.POSTGRES_PORT,
            path=self.POSTGRES_DB,
        ))

    # =============================================================================
    # MySQL 数据库配置
    # =============================================================================
    MYSQL_SERVER: str | None = Field(default=None, description="MySQL 服务器地址")
    MYSQL_PORT: int = Field(default=3306, description="MySQL 端口")
    MYSQL_USER: str | None = Field(default=None, description="MySQL 用户名")
    MYSQL_PASSWORD: str = Field(default="", description="MySQL 密码")
    MYSQL_DB: str = Field(default="", description="MySQL 数据库名")

    @computed_field
    @property
    def SQLALCHEMY_MYSQL_DATABASE_URI(self) -> str | None:
        """构建 MySQL 数据库连接 URI"""
        if all([self.MYSQL_SERVER, self.MYSQL_USER, self.MYSQL_DB]):
            return (
                f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
                f"@{self.MYSQL_SERVER}:{self.MYSQL_PORT}/{self.MYSQL_DB}"
                f"?charset=utf8mb4"
            )
        return None

    # =============================================================================
    # 邮件服务配置
    # =============================================================================
    SMTP_TLS: bool = Field(default=True, description="SMTP TLS")
    SMTP_SSL: bool = Field(default=False, description="SMTP SSL")
    SMTP_PORT: int = Field(default=587, description="SMTP 端口")
    SMTP_HOST: str | None = Field(default=None, description="SMTP 服务器")
    SMTP_USER: str | None = Field(default=None, description="SMTP 用户名")
    SMTP_PASSWORD: str | None = Field(default=None, description="SMTP 密码")
    EMAILS_FROM_EMAIL: EmailStr | None = Field(default=None, description="发件人邮箱")
    EMAILS_FROM_NAME: str | None = Field(default=None, description="发件人名称")
    EMAIL_RESET_TOKEN_EXPIRE_HOURS: int = Field(
        default=48, description="邮件重置令牌过期时间（小时）"
    )

    @computed_field
    @property
    def emails_enabled(self) -> bool:
        """检查邮件服务是否启用"""
        return bool(self.SMTP_HOST and self.EMAILS_FROM_EMAIL)

    # =============================================================================
    # 用户配置 - 改为可选并提供默认值
    # =============================================================================
    EMAIL_TEST_USER: EmailStr = Field(
        default="test@example.com", description="测试用户邮箱"
    )
    FIRST_SUPERUSER: EmailStr = Field(
        default="admin@example.com", description="第一个超级用户邮箱"
    )
    FIRST_SUPERUSER_PASSWORD: str = Field(
        default="changethis", description="第一个超级用户密码"
    )

    # =============================================================================
    # 外部服务配置
    # =============================================================================
    SENTRY_DSN: str | None = Field(default=None, description="Sentry DSN")

    # =============================================================================
    # 验证方法
    # =============================================================================
    def _check_default_secret(self, var_name: str, value: str | None) -> None:
        """检查是否使用默认密钥"""
        if value == "changethis":
            message = (
                f'变量 {var_name} 的值为 "changethis"，'
                "为了安全，请修改此值，特别是在生产环境中。"
            )
            if self.ENVIRONMENT == "local":
                warnings.warn(message, stacklevel=2)
            else:
                raise ValueError(message)

    @model_validator(mode="after")
    def _set_default_emails_from(self) -> Self:
        """设置默认发件人名称"""
        if not self.EMAILS_FROM_NAME:
            self.EMAILS_FROM_NAME = self.PROJECT_NAME
        return self

    @model_validator(mode="after")
    def _enforce_non_default_secrets(self) -> Self:
        """强制检查敏感配置"""
        if self.ENVIRONMENT != "local":  # 只在非本地环境检查
            self._check_default_secret("SECRET_KEY", self.SECRET_KEY)
            self._check_default_secret("POSTGRES_PASSWORD", self.POSTGRES_PASSWORD)
            self._check_default_secret("FIRST_SUPERUSER_PASSWORD", self.FIRST_SUPERUSER_PASSWORD)
        return self


# 创建配置实例，包含调试信息
def create_settings():
    """创建设置实例并显示调试信息"""
    print("正在加载配置...")

    # 显示当前工作目录
    print(f"当前工作目录: {os.getcwd()}")

    # 检查可能的 .env 文件位置
    current_file = Path(__file__)
    possible_env_files = [
        Path.cwd() / ".env",
        current_file.parent / ".env",
        current_file.parent.parent / ".env",
        current_file.parent.parent.parent / ".env",
    ]

    print("检查 .env 文件位置:")
    for env_file in possible_env_files:
        exists = env_file.exists()
        print(f"  {env_file}: {'✓ 存在' if exists else '✗ 不存在'}")
        if exists:
            print(f"    文件大小: {env_file.stat().st_size} bytes")

    # 检查关键环境变量
    key_vars = ['PROJECT_NAME', 'POSTGRES_SERVER', 'POSTGRES_USER', 'FIRST_SUPERUSER']
    print("检查环境变量:")
    for var in key_vars:
        value = os.getenv(var)
        print(f"  {var}: {'✓ 已设置' if value else '✗ 未设置'}")

    try:
        settings = Settings()
        print("✓ 配置加载成功!")
        return settings
    except Exception as e:
        print(f"✗ 配置加载失败: {e}")
        raise


# 根据是否为主模块决定是否显示调试信息
# if __name__ == "__main__":
#     settings = create_settings()
# else:
#     settings = Settings()

settings = Settings()
