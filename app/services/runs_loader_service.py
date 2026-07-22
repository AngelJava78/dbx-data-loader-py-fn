import logging

from app.clients.databricks_client import DatabricksClient
from app.config.settings import Settings
from app.repositories.runs_repository import RunsRepository
from app.repositories.tasks_repository import TasksRepository
from app.services.run_transformer import RunTransformer
from app.services.task_transformer import TaskTransformer


class RunsLoaderService:
    """Coordina la extracción, transformación y carga de runs."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.repository = RunsRepository(settings.postgres)
        self.task_repository = TasksRepository(settings.postgres)

    def execute(self) -> int:
        started_date = self.repository.get_max_started_cdmx()
        logging.info("Fecha de inicio: %s", started_date)
        with DatabricksClient(self.settings.databricks) as client:
            runs = client.get_runs(started_date)

        records = []
        records_tasks = []
        for run in runs:
            try:
                run_item = RunTransformer.transform(run)
                tasks = client.get_tasks(run_item.run_id)
                tasks_list = []
                for task in tasks:
                    tasks_list.append(TaskTransformer.transform(run_item.run_id, task))
                run_item.tasks = tasks_list
                records.append(run_item)
                records_tasks.extend(tasks_list)
            except ValueError as exc:
                logging.warning("Run omitido: %s", exc)

        total_runs = self.repository.save_all(records)
        total_tasks = self.task_repository.save_all(records_tasks)
        return total_runs + total_tasks

