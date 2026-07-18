from collections.abc import Sequence

import psycopg2
from psycopg2.extras import execute_values

from app.config.settings import PostgresSettings
from app.models.run_record import RunRecord


class RunsRepository:
    INSERT_SQL = """
    INSERT INTO public.runs
    (
        run_id, job_id, job_name, started_cdmx, ended_cdmx, duration,
        run_type, result_state, termination_code, workspace_id,
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
        process_id = EXCLUDED.process_id,
        subprocess_id = EXCLUDED.subprocess_id,
        stage_id = EXCLUDED.stage_id,
        substage_id = EXCLUDED.substage_id,
        username = EXCLUDED.username,
        folio_number = EXCLUDED.folio_number,
        parameter_source = EXCLUDED.parameter_source
    """

    def __init__(self, settings: PostgresSettings) -> None:
        self.settings = settings

    def save_all(self, records: Sequence[RunRecord]) -> int:
        if not records:
            return 0

        values = [record.as_tuple() for record in records]

        with psycopg2.connect(
            host=self.settings.host,
            port=self.settings.port,
            dbname=self.settings.database,
            user=self.settings.user,
            password=self.settings.password,
            connect_timeout=10,
            application_name="databricks-runs-loader",
        ) as connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    self.INSERT_SQL,
                    values,
                    page_size=self.settings.batch_size,
                )

        return len(records)
