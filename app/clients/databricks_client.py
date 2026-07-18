import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests

from app.config.settings import DatabricksSettings

UTC_TZ = ZoneInfo("UTC")


class DatabricksClient:
    """Cliente para consultar runs de la API Jobs de Databricks."""

    def __init__(self, settings: DatabricksSettings) -> None:
        self.settings = settings

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {settings.token}",
                "Accept": "application/json",
            }
        )

    def _get_query_window(self) -> tuple[int, int]:
        """Obtiene el rango de consulta en epoch milliseconds."""

        end_utc = datetime.now(tz=UTC_TZ)
        start_utc = end_utc - timedelta(
            minutes=self.settings.lookback_minutes
        )

        start_ms = int(start_utc.timestamp() * 1000)
        end_ms = int(end_utc.timestamp() * 1000)

        return start_ms, end_ms

    def get_runs(self) -> list[dict[str, Any]]:
        """Obtiene todos los runs dentro del rango configurado."""

        start_ms, end_ms = self._get_query_window()

        all_runs: list[dict[str, Any]] = []
        page_token: str | None = None

        while True:
            params: dict[str, int | str] = {
                "start_time_from": start_ms,
                "start_time_to": end_ms,
                "limit": self.settings.page_size,
            }

            if page_token:
                params["page_token"] = page_token

            response = self.session.get(
                f"{self.settings.host}/api/2.1/jobs/runs/list",
                params=params,
                timeout=60,
            )

            response.raise_for_status()

            payload = response.json()
            runs = payload.get("runs", [])

            all_runs.extend(runs)

            logging.info(
                "Página recuperada: %s runs. Total acumulado: %s.",
                len(runs),
                len(all_runs),
            )

            if not payload.get("has_more", False):
                break

            page_token = payload.get("next_page_token")

            if not page_token:
                logging.warning(
                    "Databricks devolvió has_more=true "
                    "sin next_page_token."
                )
                break

        return all_runs

    def close(self) -> None:
        """Cierra la sesión HTTP."""

        self.session.close()

    def __enter__(self) -> "DatabricksClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

