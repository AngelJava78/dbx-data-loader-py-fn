from datetime import datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo
import logging
from app.models.task_record import TaskRecord

CDMX_TZ = ZoneInfo("America/Mexico_City")


class TaskTransformer:
    """Convierte las tareas de Databricks en registros para PostgreSQL."""

    @staticmethod
    def safe_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def get_parameters(cls, task: dict[str, Any]) -> tuple[dict[str, Any], str]:

        job_parameters = task.get("job_parameters") or {}

        if isinstance(job_parameters, dict) and job_parameters:
            return job_parameters, "JOB_PARAMETERS"

        if isinstance(job_parameters, list):
            parameters = {
                item["name"]: item.get("value")
                for item in job_parameters
                if isinstance(item, dict) and item.get("name")
            }
            if parameters:
                return parameters, "JOB_PARAMETERS"

        base_parameters = task.get("base_parameters") or {}

        if isinstance(base_parameters, dict) and base_parameters:
            return base_parameters, "BASE_PARAMETERS"

        if isinstance(base_parameters, list):
            parameters = {
                item["name"]: item.get("value")
                for item in base_parameters
                if isinstance(item, dict) and item.get("name")
            }
            if parameters:
                return parameters, "BASE_PARAMETERS"

        return {}, "NO_PARAMETERS"

    @classmethod
    def epoch_ms_to_cdmx(cls, value: Any) -> datetime | None:
        milliseconds = cls.safe_int(value)

        if milliseconds is None or milliseconds <= 0:
            return None

        return datetime.fromtimestamp(
            milliseconds / 1000,
            tz=CDMX_TZ,
        )

    @classmethod
    def calculate_duration(
        cls,
        start_time: Any,
        end_time: Any,
    ) -> timedelta | None:
        start_ms = cls.safe_int(start_time)
        end_ms = cls.safe_int(end_time)

        if start_ms is None or end_ms is None:
            return None

        duration_ms = end_ms - start_ms

        if duration_ms < 0:
            raise ValueError(
                "The end time cannot be earlier than the start time. "
                f"start_time={start_ms}, end_time={end_ms}"
            )

        return timedelta(milliseconds=duration_ms)

    @classmethod
    def get_task_timing(
        cls,
        task: dict[str, Any],
    ) -> tuple[datetime | None, datetime | None, timedelta | None]:
        start_ms = cls.safe_int(task.get("start_time"))
        end_ms = cls.safe_int(task.get("end_time"))

        started_cdmx = cls.epoch_ms_to_cdmx(start_ms)
        ended_cdmx = cls.epoch_ms_to_cdmx(end_ms)

        # La tarea nunca inició.
        if start_ms is None or start_ms <= 0:
            return ended_cdmx, ended_cdmx, timedelta(0)

        # La tarea todavía no terminó o no tiene fin válido.
        if end_ms is None or end_ms <= 0:
            return started_cdmx, started_cdmx, timedelta(0)

        duration_ms = end_ms - start_ms

        if duration_ms < 0:
            raise ValueError(
                "The task end time cannot be earlier than the start time. "
                f"task_run_id={task.get('run_id')}, "
                f"start_time={start_ms}, end_time={end_ms}"
            )

        return (
            started_cdmx,
            ended_cdmx,
            timedelta(milliseconds=duration_ms),
        )

    @classmethod
    def transform(cls, run_id: int, task: dict[str, Any]) -> TaskRecord:
        task_run_id = cls.safe_int(task.get("run_id"))

        if task_run_id is None:
            raise ValueError("La tarea no contiene un run_id válido.")

        parameters: dict[str, Any] = {}
        parameter_source: str | None = None

        # Estado de la tarea
        state = task.get("state") or {}

        life_cycle_state = (state.get("life_cycle_state") or "").upper()

        result_state = (state.get("result_state") or "").upper()

        status = result_state or life_cycle_state or None

        if life_cycle_state == "SKIPPED":
            status = "SKIPPED"

        # Fechas y duración
        started_cdmx, ended_cdmx, duration = cls.get_task_timing(task)

        task_type = ""
        notebook_path = ""
        notebook_name = ""

        # Notebook task
        notebook_task = task.get("notebook_task") or {}

        if notebook_task:
            task_type = "notebook_task"

            notebook_path = notebook_task.get("notebook_path") or ""

            parameters, parameter_source = cls.get_parameters(notebook_task)

            if notebook_path:
                notebook_name = notebook_path.rsplit("/", 1)[-1]

        # Run job task
        run_job_task = task.get("run_job_task") or {}

        if run_job_task:
            task_type = "job_task"
            notebook_path = ""
            notebook_name = ""

            parameters, parameter_source = cls.get_parameters(run_job_task)

        # Condition task
        condition_task = task.get("condition_task") or {}

        if condition_task:
            task_type = "condition_task"
            notebook_path = ""
            notebook_name = ""

        return TaskRecord(
            run_id=run_id,
            task_run_id=task_run_id,
            task_key=task.get("task_key"),
            started_cdmx=started_cdmx,
            ended_cdmx=ended_cdmx,
            duration=duration,
            task_type=task_type,
            run_page_url = task.get("run_page_url") or "",
            status=status,
            notebook_path=notebook_path,
            notebook_name=notebook_name,
            process_id=cls.safe_int(parameters.get("sr_proceso")),
            subprocess_id=cls.safe_int(parameters.get("sr_subproceso")),
            stage_id=cls.safe_int(parameters.get("sr_etapa")),
            substage_id=cls.safe_int(parameters.get("sr_subetapa")),
            username=parameters.get("sr_usuario") or "",
            folio_number=parameters.get("sr_folio") or "",
            parameter_source=parameter_source,
        )
