from collections.abc import Sequence
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import connection
from app.config.settings import PostgresSettings
from app.models.run_record import RunRecord


class RunsRepository:
    INSERT_SQL = """
    INSERT INTO public.runs
    (
        run_id, job_id, job_name, started_cdmx, ended_cdmx, duration,
        run_type, result_state, termination_code, workspace_id, run_page_url,
        process_id, subprocess_id, stage_id, substage_id,
        username, folio_number, parameter_source
    )
    VALUES %s
    ON CONFLICT (run_id)
    DO UPDATE SET
        job_id = EXCLUDED.job_id,
        job_name = EXCLUDED.job_name,
        started_cdmx = EXCLUDED.started_cdmx,
        ended_cdmx = EXCLUDED.ended_cdmx,
        duration = EXCLUDED.duration,
        run_type = EXCLUDED.run_type,
        result_state = EXCLUDED.result_state,
        termination_code = EXCLUDED.termination_code,
        workspace_id = EXCLUDED.workspace_id,
        run_page_url = EXCLUDED.run_page_url,
        process_id = EXCLUDED.process_id,
        subprocess_id = EXCLUDED.subprocess_id,
        stage_id = EXCLUDED.stage_id,
        substage_id = EXCLUDED.substage_id,
        username = EXCLUDED.username,
        folio_number = EXCLUDED.folio_number,
        parameter_source = EXCLUDED.parameter_source
    """

    SELECT_MAX_STARTED_SQL = """
        SELECT COALESCE(MAX(started_cdmx), DATE('2026-07-01'))
        FROM public.runs
    """

    def __init__(self, settings: PostgresSettings) -> None:
        self.settings = settings

    def get_connection(self) -> connection:
        return psycopg2.connect(
            host=self.settings.host,
            port=self.settings.port,
            dbname=self.settings.database,
            user=self.settings.user,
            password=self.settings.password,
            connect_timeout=10,
            application_name="databricks-runs-loader",
        )  

    def get_max_started_cdmx(self) -> datetime | None:
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(self.SELECT_MAX_STARTED_SQL)
                result = cursor.fetchone()

        if result is None:
            return None

        return result[0]          

    def save_all(self, records: Sequence[RunRecord]) -> int:
        if not records:
            return 0

        values = [record.as_tuple() for record in records]

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    self.INSERT_SQL,
                    values,
                    page_size=self.settings.batch_size,
                )

        return len(records)

