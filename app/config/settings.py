import os
from dataclasses import dataclass

class ConfigurationError(ValueError):
    """Indica que una variable de entorno requerida es inválida o no existe."""


def required_setting(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ConfigurationError(f"Falta la configuración obligatoria: {name}")
    return value


def int_setting(name: str, default: int) -> int:
    value = os.getenv(name)
    if value in (None, ""):
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} debe ser un número entero.") from exc


@dataclass(frozen=True)
class DatabricksSettings:
    host: str
    token: str
    page_size: int
    lookback_minutes: int


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    batch_size: int


@dataclass(frozen=True)
class Settings:
    databricks: DatabricksSettings
    postgres: PostgresSettings

    @classmethod
    def from_environment(cls) -> "Settings":
        lookback_minutes = int_setting("LOOKBACK_MINUTES", 30)
        page_size = int_setting("DATABRICKS_PAGE_SIZE", 100)
        batch_size = int_setting("POSTGRES_BATCH_SIZE", 500)

        if lookback_minutes <= 0:
            raise ConfigurationError("LOOKBACK_MINUTES debe ser mayor que cero.")
        if not 1 <= page_size <= 100:
            raise ConfigurationError("DATABRICKS_PAGE_SIZE debe estar entre 1 y 100.")
        if batch_size <= 0:
            raise ConfigurationError("POSTGRES_BATCH_SIZE debe ser mayor que cero.")

        return cls(
            databricks=DatabricksSettings(
                host=required_setting("DATABRICKS_HOST").rstrip("/"),
                token=required_setting("DATABRICKS_TOKEN"),
                page_size=page_size,
                lookback_minutes=lookback_minutes,
            ),
            postgres=PostgresSettings(
                host=required_setting("POSTGRES_HOST"),
                port=int_setting("POSTGRES_PORT", 5432),
                database=required_setting("POSTGRES_DATABASE"),
                user=required_setting("POSTGRES_USER"),
                password=required_setting("POSTGRES_PASSWORD"),
                batch_size=batch_size,
            ),
        )
