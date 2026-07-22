import logging
import azure.functions as func
from app.config.settings import Settings
from app.services.runs_loader_service import RunsLoaderService

app = func.FunctionApp()

@app.timer_trigger(
        schedule="0 */5 * * * *", 
        arg_name="timer", 
        run_on_startup=True,
        use_monitor=True
) 
def databricks_runs_loader(timer: func.TimerRequest) -> None:
    """Carga periódicamente los runs de Databricks en PostgreSQL."""
    if timer.past_due:
        logging.warning("La ejecución programada está retrasada.")

    try:
        settings = Settings.from_environment()
        inserted_or_updated = RunsLoaderService(settings).execute()
        logging.info(
            "Carga finalizada. Registros insertados o actualizados: %s",
            inserted_or_updated,
        )
    except Exception:
        logging.exception("Falló la carga de runs de Databricks.")
        raise