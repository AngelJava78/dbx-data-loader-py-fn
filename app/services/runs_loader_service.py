import logging

from app.clients.databricks_client import DatabricksClient
from app.config.settings import Settings
from app.repositories.runs_repository import RunsRepository
from app.services.run_transformer import RunTransformer


class RunsLoaderService:
    """Coordina la extracción, transformación y carga de runs."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.repository = RunsRepository(settings.postgres)

    def execute(self) -> int:
        started_date = self.repository.get_max_started_cdmx()
        with DatabricksClient(self.settings.databricks) as client:
            runs = client.get_runs(started_date)

        records = []
        for run in runs:
            try:
                records.append(RunTransformer.transform(run))
            except ValueError as exc:
                logging.warning("Run omitido: %s", exc)

        return self.repository.save_all(records)
