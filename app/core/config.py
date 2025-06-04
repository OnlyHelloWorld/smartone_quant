import secrets
import warnings
from typing import Annotated, Any, Literal

from pydantic import (
    AnyUrl,
    BeforeValidator,
    EmailStr,
    HttpUrl,
    PostgresDsn,
    computed_field,
    model_validator,
)
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self


def parse_cors(v: Any) -> list[str] | str:
    """
    解析 CORS 配置。
    :param v: 参数值，可以是字符串或列表。
    :return:  如果是字符串且不以 "[" 开头，则将其拆分为列表；如果是列表或字符串，则直接返回。
    """
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",")]
    elif isinstance(v, list | str):
        return v
    raise ValueError(v)


class Settings(BaseSettings):
    """
    应用程序配置类，使用 Pydantic 进行设置管理。
    包含数据库连接、CORS、邮件服务等配置。
    """
    # 使用顶层 .env 文件（位于 ./backend/ 上一级目录）
    model_config = SettingsConfigDict(
        env_file="../.env",
        # 忽略空环境变量
        env_ignore_empty=True,
        # 允许额外的环境变量
        extra="ignore",
    )
    # API 路径前缀
    API_V1_STR: str = "/api/v1"
    # 安全密钥，用于加密和签名
    SECRET_KEY: str = secrets.token_urlsafe(32)
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    # 前端主机地址
    FRONTEND_HOST: str = "http://localhost:5173"
    # 环境类型：本地、预发布或生产，默认为本地
    ENVIRONMENT: Literal["local", "staging", "production"] = "local"
    # 后端 CORS 配置，支持字符串或列表格式
    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str, BeforeValidator(parse_cors)
    ] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def all_cors_origins(self) -> list[str]:
        """
        获取所有 CORS 允许的源地址列表。
        :return: list[str]: 返回一个字符串列表，包含所有允许的源地址。
        """
        return [str(origin).rstrip("/") for origin in self.BACKEND_CORS_ORIGINS] + [
            self.FRONTEND_HOST
        ]
    # 项目名称
    PROJECT_NAME: str
    # Sentry 错误跟踪服务的 DSN
    SENTRY_DSN: HttpUrl | None = None
    # PostgreSQL 数据库服务器地址配置
    POSTGRES_SERVER: str
    # PostgreSQL 数据库端口，默认为 5432
    POSTGRES_PORT: int = 5432
    # PostgreSQL 数据库用户名
    POSTGRES_USER: str
    # PostgreSQL 数据库密码，默认为空字符串
    POSTGRES_PASSWORD: str = ""
    # PostgreSQL 数据库名称，默认为空字符串
    POSTGRES_DB: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> PostgresDsn:
        """
        构建 SQLAlchemy 数据库连接 URI。
        :return: PostgresDsn: 返回一个 PostgresDsn 对象，包含数据库连接信息。
        """
        return MultiHostUrl.build(
            scheme="postgresql+psycopg",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_SERVER,
            port=self.POSTGRES_PORT,
            path=self.POSTGRES_DB,
        )

    # MySQL 数据库服务器地址配置
    MYSQL_SERVER: str | None = None
    # MySQL 数据库端口，默认为 3306
    MYSQL_PORT: int = 3306
    # MySQL 数据库用户名
    MYSQL_USER: str | None = None
    # MySQL 数据库密码，默认为空字符串
    MYSQL_PASSWORD: str = ""
    # MySQL 数据库名称，默认为空字符串
    MYSQL_DB: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_MYSQL_DATABASE_URI(self) -> str | None:
        """
        构建 SQLAlchemy MySQL 数据库连接 URI。
        :return: str | None: 返回一个 MySQL 连接字符串，若未配置则返回 None。
        """
        if self.MYSQL_SERVER and self.MYSQL_USER and self.MYSQL_DB:
            return f"mysql+pymysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}@{self.MYSQL_SERVER}:{self.MYSQL_PORT}/{self.MYSQL_DB}"
        return None
    # SMTP 邮件服务器是否启用传输层安全协议TLS
    SMTP_TLS: bool = True
    # SMTP 邮件服务器是否启用安全套接层SSL
    SMTP_SSL: bool = False
    # SMTP 邮件服务器端口，默认为 587
    SMTP_PORT: int = 587
    # SMTP 邮件服务器地址, None 表示未设置
    SMTP_HOST: str | None = None
    # SMTP 邮件服务器用户名, None 表示未设置
    SMTP_USER: str | None = None
    # SMTP 邮件服务器密码, None 表示未设置
    SMTP_PASSWORD: str | None = None
    # 发件人邮箱地址, None 表示未设置
    EMAILS_FROM_EMAIL: EmailStr | None = None
    # 发件人名称, None 表示未设置
    EMAILS_FROM_NAME: EmailStr | None = None

    @model_validator(mode="after")
    def _set_default_emails_from(self) -> Self:
        """
        设置默认的发件人名称，如果未设置则使用项目名称。
        :return: Self: 返回当前设置实例。
        """
        if not self.EMAILS_FROM_NAME:
            self.EMAILS_FROM_NAME = self.PROJECT_NAME
        return self
    # 邮件重置令牌过期时间，单位为小时
    EMAIL_RESET_TOKEN_EXPIRE_HOURS: int = 48

    @computed_field  # type: ignore[prop-decorator]
    @property
    def emails_enabled(self) -> bool:
        """
        检查邮件服务是否启用。
        :return: bool: 如果 SMTP_HOST 和 EMAILS_FROM_EMAIL 都已设置，则返回 True，否则返回 False。
        """
        return bool(self.SMTP_HOST and self.EMAILS_FROM_EMAIL)
    # 测试用户邮箱地址
    EMAIL_TEST_USER: EmailStr = "test@example.com"
    # 第一个超级用户的邮箱地址
    FIRST_SUPERUSER: EmailStr
    # 第一个超级用户的密码
    FIRST_SUPERUSER_PASSWORD: str

    def _check_default_secret(self, var_name: str, value: str | None) -> None:
        """
        检查是否使用了默认的安全密钥或密码。
        :param var_name: str, 变量名称，用于日志记录。
        :param value: str | None, 变量值，检查是否为默认值 "changethis"。
        :return:
        """
        if value == "changethis":
            message = (
                f'The value of {var_name} is "changethis", '
                "for security, please change it, at least for deployments."
            )
            if self.ENVIRONMENT == "local":
                warnings.warn(message, stacklevel=1)
            else:
                raise ValueError(message)

    @model_validator(mode="after")
    def _enforce_non_default_secrets(self) -> Self:
        """
        强制检查所有敏感配置项是否使用了非默认值。
        :return: Self: 返回当前设置实例。
        """
        # 检查 SECRET_KEY、POSTGRES_PASSWORD 和 FIRST_SUPERUSER_PASSWORD 是否使用了默认值
        self._check_default_secret("SECRET_KEY", self.SECRET_KEY)
        self._check_default_secret("POSTGRES_PASSWORD", self.POSTGRES_PASSWORD)
        self._check_default_secret(
            "FIRST_SUPERUSER_PASSWORD", self.FIRST_SUPERUSER_PASSWORD
        )

        return self

# 创建 Settings 实例
settings = Settings()  # type: ignore
