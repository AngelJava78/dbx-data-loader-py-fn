from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from app.models.run_record import RunRecord

CDMX_TZ = ZoneInfo("America/Mexico_City")


class RunTransformer:
    """Convierte la respuesta de Databricks en registros para PostgreSQL."""

    @staticmethod
    def safe_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def get_parameters(cls, run: dict[str, Any]) -> tuple[dict[str, Any], str]:
        job_parameters = run.get("job_parameters") or []
        if job_parameters:
            parameters = {
                item.get("name"): item.get("value")
                for item in job_parameters
                if item.get("name")
            }
            return parameters, "JOB_PARAMETERS"

        notebook_params = (
            (run.get("overriding_parameters") or {}).get("notebook_params") or {}
        )
        if notebook_params:
            return notebook_params, "OVERRIDING_PARAMETERS"

        return {}, "NO_PARAMETERS"

    @classmethod
    def epoch_ms_to_cdmx(cls, value: Any) -> datetime | None:
        milliseconds = cls.safe_int(value)
        if milliseconds is None:
            return None
        return datetime.fromtimestamp(milliseconds / 1000, tz=CDMX_TZ)

    @classmethod
    def calculate_duration(cls, start_time: Any, end_time: Any) -> str | None:
        start_ms = cls.safe_int(start_time)
        end_ms = cls.safe_int(end_time)
        if start_ms is None or end_ms is None:
            return None

        total_seconds = max(0, (end_ms - start_ms) // 1000)
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02}:{minutes:02}:{seconds:02}"

    @classmethod
    def extract_workspace_id(cls, run_page_url: str | None) -> int | None:
        if not run_page_url:
            return None
        values = parse_qs(urlparse(run_page_url).query).get("o")
        return cls.safe_int(values[0]) if values else None

    @classmethod
    def transform(cls, run: dict[str, Any]) -> RunRecord:
        run_id = cls.safe_int(run.get("run_id"))
        if run_id is None:
            raise ValueError("El run no contiene un run_id válido.")

        parameters, parameter_source = cls.get_parameters(run)
        state = run.get("state") or {}
        status = run.get("status") or {}
        termination_details = status.get("termination_details") or {}

        return RunRecord(
            run_id=run_id,
            job_id=cls.safe_int(run.get("job_id")),
            job_name=run.get("run_name"),
            started_cdmx=cls.epoch_ms_to_cdmx(run.get("start_time")),
            ended_cdmx=cls.epoch_ms_to_cdmx(run.get("end_time")),
            duration=cls.calculate_duration(run.get("start_time"), run.get("end_time")),
            run_type=run.get("run_type"),
            result_state=state.get("result_state"),
            termination_code=termination_details.get("code"),
            workspace_id=cls.extract_workspace_id(run.get("run_page_url")),
            process_id=cls.safe_int(parameters.get("sr_proceso")),
            subprocess_id=cls.safe_int(parameters.get("sr_subproceso")),
            stage_id=cls.safe_int(parameters.get("sr_etapa")),
            substage_id=cls.safe_int(parameters.get("sr_subetapa")),
            username=parameters.get("sr_usuario") or "",
            folio_number=parameters.get("sr_folio") or "",
            parameter_source=parameter_source,
        )
