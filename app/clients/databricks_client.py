import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests

from app.config.settings import DatabricksSettings

UTC_TZ = ZoneInfo("UTC")
CDMX_TZ = ZoneInfo("America/Mexico_City")

RUNS_LIST_ENDPOINT = "/api/2.2/jobs/runs/list"
RUNS_GET_ENDPOINT = "/api/2.2/jobs/runs/get"

MAX_RUNS_PAGE_SIZE = 25


class DatabricksClient:
    """Cliente para consultar runs y tasks mediante Databricks Jobs API 2.2."""

    def __init__(self, settings: DatabricksSettings) -> None:
        self.settings = settings

        if not 1 <= self.settings.page_size <= MAX_RUNS_PAGE_SIZE:
            raise ValueError(
                "Databricks page_size must be between " f"1 and {MAX_RUNS_PAGE_SIZE}."
            )

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {settings.token}",
                "Accept": "application/json",
            }
        )

    @staticmethod
    def cdmx_to_epoch_ms(date: datetime) -> int:
        """
        Interpreta una fecha sin zona horaria como CDMX
        y la convierte a epoch milliseconds UTC.
        """

        if date.tzinfo is None:
            cdmx_date = date.replace(tzinfo=CDMX_TZ)
        else:
            cdmx_date = date.astimezone(CDMX_TZ)

        return int(cdmx_date.timestamp() * 1000)

    def get_runs(
        self, start_date: datetime, end_date: datetime
    ) -> list[dict[str, Any]]:
        """
        Obtiene todos los runs cuya fecha de inicio pertenece
        al intervalo lógico [start_date, end_date).
        """

        if end_date <= start_date:
            raise ValueError("end_date must be greater than start_date.")

        start_time_from_ms = self.cdmx_to_epoch_ms(start_date)
        end_time_exclusive_ms = self.cdmx_to_epoch_ms(end_date)
        start_time_to_ms = end_time_exclusive_ms - 1

        logging.info(
            "Fetching Databricks runs for CDMX interval [%s, %s). "
            "Epoch interval: start_time_from=%s, start_time_to=%s.",
            start_date,
            end_date,
            start_time_from_ms,
            start_time_to_ms,
        )

        all_runs: list[dict[str, Any]] = []
        page_token: str | None = None
        page_number = 1

        while True:
            params: dict[str, int | str] = {
                "start_time_from": start_time_from_ms,
                "start_time_to": start_time_to_ms,
                "limit": self.settings.page_size,
            }

            if page_token:
                params["page_token"] = page_token

            response = self.session.get(
                f"{self.settings.host}{RUNS_LIST_ENDPOINT}",
                params=params,
                timeout=60,
            )
            # https://adb-3925217763478917.17.azuredatabricks.net/api/2.2/jobs/runs/list?start_time_from=1783576800000&start_time_to=1783663200000&limit=25

            response.raise_for_status()

            payload: dict[str, Any] = response.json()
            runs: list[dict[str, Any]] = payload.get("runs") or []

            all_runs.extend(runs)

            logging.info(
                "Databricks runs page %s retrieved. "
                "Page records: %s. Accumulated records: %s.",
                page_number,
                len(runs),
                len(all_runs),
            )

            # if not payload.get("has_more", False):
            #     break

            next_page_token = payload.get("next_page_token")

            if not next_page_token:
                break

            page_token = next_page_token
            page_number += 1

        logging.info(
            "Databricks runs query completed. Total runs retrieved: %s.",
            len(all_runs),
        )

        return all_runs

    def get_tasks(self, run_id: int) -> list[dict[str, Any]]:
        """
        Obtiene todas las tareas pertenecientes a un run.

        Jobs API 2.2 puede paginar el arreglo tasks cuando el run
        contiene más de 100 tareas.
        """

        if run_id <= 0:
            raise ValueError("run_id must be greater than zero.")

        logging.info(
            "Fetching Databricks tasks for run_id=%s.",
            run_id,
        )

        all_tasks: list[dict[str, Any]] = []
        page_token: str | None = None
        page_number = 1

        while True:
            params: dict[str, int | str] = {
                "run_id": run_id,
            }

            if page_token is not None:
                params["page_token"] = page_token

            response = self.session.get(
                f"{self.settings.host}{RUNS_GET_ENDPOINT}",
                params=params,
                timeout=60,
            )
            response.raise_for_status()

            payload: dict[str, Any] = response.json()
            tasks: list[dict[str, Any]] = payload.get("tasks") or []

            all_tasks.extend(tasks)

            logging.info(
                "Databricks tasks page %s retrieved for run_id=%s. "
                "Page records: %s. Accumulated records: %s.",
                page_number,
                run_id,
                len(tasks),
                len(all_tasks),
            )

            next_page_token = payload.get("next_page_token")

            if not next_page_token:
                break

            page_token = next_page_token
            page_number += 1

        logging.info(
            "Databricks tasks query completed for run_id=%s. "
            "Total tasks retrieved: %s.",
            run_id,
            len(all_tasks),
        )

        return all_tasks

    def close(self) -> None:
        """Cierra la sesión HTTP."""

        self.session.close()

    def __enter__(self) -> "DatabricksClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()