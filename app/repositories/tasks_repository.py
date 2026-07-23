from collections.abc import Sequence
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import connection
from app.config.settings import PostgresSettings
from app.models.run_record import TaskRecord


class TasksRepository:

    INSERT_SQL = """
    INSERT INTO public.tasks
    (
        task_run_id,
        run_id,
        task_key,
        started_cdmx,
        ended_cdmx,
        duration,
        task_type,
        run_page_url,
        status,
        notebook_path,
        notebook_name,
        process_id,
        subprocess_id,
        stage_id,
        substage_id,
        username,
        folio_number,
        parameter_source
    )
    VALUES %s
    ON CONFLICT (task_run_id)
    DO UPDATE SET
        run_id = EXCLUDED.run_id,    
        task_key = EXCLUDED.task_key,
        started_cdmx = EXCLUDED.started_cdmx,
        ended_cdmx = EXCLUDED.ended_cdmx,
        duration = EXCLUDED.duration,
        task_type = EXCLUDED.task_type,
        run_page_url = EXCLUDED.run_page_url,
        status = EXCLUDED.status,
        notebook_path = EXCLUDED.notebook_path,
        notebook_name = EXCLUDED.notebook_name,
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

    def _get_connection(self) -> connection:
        return psycopg2.connect(
            host=self.settings.host,
            port=self.settings.port,
            dbname=self.settings.database,
            user=self.settings.user,
            password=self.settings.password,
            connect_timeout=10,
            application_name="databricks-runs-loader",
        )  

    def save_all(self, records: Sequence[TaskRecord]) -> int:
        if not records:
            return 0

        values = [record.as_tuple() for record in records]

        with self._get_connection() as connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    self.INSERT_SQL,
                    values,
                    page_size=self.settings.batch_size,
                )

        return len(records)
