import logging
import azure.functions as func
from app.config.settings import Settings
from app.services.runs_loader_service import RunsLoaderService
from datetime import datetime
from time import perf_counter
from zoneinfo import ZoneInfo

app = func.FunctionApp()

CDMX_TZ = ZoneInfo("America/Mexico_City")


@app.timer_trigger(
    schedule="0 */5 * * * *", arg_name="timer", run_on_startup=True, use_monitor=True
)
def databricks_runs_loader(timer: func.TimerRequest) -> None:
    """Carga periódicamente los runs de Databricks en PostgreSQL."""

    started_at = datetime.now(CDMX_TZ)
    started_counter = perf_counter()
    logging.info(
        "Inició la carga de runs de Databricks a las %s.",
        started_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
    )
    if timer.past_due:
        logging.warning("La ejecución programada está retrasada.")

    try:
        settings = Settings.from_environment()
        inserted_or_updated = RunsLoaderService(settings).execute()

        finished_at = datetime.now(CDMX_TZ)
        elapsed_seconds = perf_counter() - started_counter

        logging.info(
            "Carga finalizada correctamente. "
            "Inicio: %s. Fin: %s. Duración: %.2f segundos. "
            "Registros insertados o actualizados: %s.",
            started_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
            finished_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
            elapsed_seconds,
            inserted_or_updated,
        )
    except Exception:
        finished_at = datetime.now(CDMX_TZ)
        elapsed_seconds = perf_counter() - started_counter

        logging.exception(
            "Falló la carga de runs de Databricks. "
            "Inicio: %s. Fin: %s. Duración antes del error: %.2f segundos.",
            started_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
            finished_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
            elapsed_seconds,
        )
        raise
