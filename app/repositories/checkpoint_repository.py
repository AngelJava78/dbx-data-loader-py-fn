from collections.abc import Sequence
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import connection
from app.config.settings import PostgresSettings


class CheckpointRepository:
    PROCESS_NAME = "databricks_runs_loader"
    SELECT_SQL = """
        SELECT COALESCE(
            (
                SELECT processed_until
                FROM public.checkpoints
                WHERE process_name = %s
            ),
            %s::timestamptz
        ) AS processed_until;
    """

    UPDATE_SQL = """
        UPDATE public.checkpoints
        SET 
            processed_until = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE
            process_name = %s;
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

    def get_processed_until(self) -> datetime:
        default_processed_until = datetime.fromisoformat("2026-07-01T00:00:00-06:00")

        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    self.SELECT_SQL,
                    (self.PROCESS_NAME, default_processed_until),
                )
                result = cursor.fetchone()

        if result is None:
            raise RuntimeError("The checkpoint query did not return a result.")
        return result[0]

    def update_processed_until(self, processed_until: datetime) -> None:
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    self.UPDATE_SQL,
                    (
                        processed_until,
                        self.PROCESS_NAME,
                    ),
                )

                if cursor.rowcount == 0:
                    raise RuntimeError(
                        "The checkpoint was not updated because it does not exist "
                        "or the new value is not greater than the current value."
                    )
