import logging
from datetime import datetime, timedelta

from app.clients.databricks_client import DatabricksClient
from app.config.settings import Settings
from app.repositories.checkpoint_repository import CheckpointRepository
from app.repositories.runs_repository import RunsRepository
from app.repositories.tasks_repository import TasksRepository
from app.services.run_transformer import RunTransformer
from app.services.task_transformer import TaskTransformer


class RunsLoaderService:
    """Coordina la extracción, transformación y carga de runs."""

    WINDOW_SIZE = timedelta(hours=24)
    OVERLAP = timedelta(minutes=10)
    MAX_TASK_DURATION = timedelta(days=7)

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.runs_repository = RunsRepository(settings.postgres)
        self.tasks_repository = TasksRepository(settings.postgres)
        self.checkpoint_repository = CheckpointRepository(settings.postgres)

    def execute(self) -> int:
        checkpoint = self.checkpoint_repository.get_processed_until()

        now = datetime.now(tz=checkpoint.tzinfo) - timedelta(minutes=1)

        checkpoint_target = min(
            checkpoint + self.WINDOW_SIZE,
            now,
        )

        if checkpoint_target <= checkpoint:
            logging.info(
                "No new interval is available. Current checkpoint: %s.",
                checkpoint,
            )
            return 0

        query_end = min(
            checkpoint_target + self.OVERLAP,
            now,
        )

        logging.info(
            "Processing Databricks runs. "
            "Query interval: [%s, %s). "
            "Current checkpoint: %s. "
            "Next checkpoint: %s.",
            checkpoint,
            query_end,
            checkpoint,
            checkpoint_target,
        )

        run_records = []
        task_records = []

        with DatabricksClient(self.settings.databricks) as client:
            runs = client.get_runs(
                start_date=checkpoint,
                end_date=query_end,
            )

            logging.info(
                "Databricks returned %s runs.",
                len(runs),
            )

            for run in runs:
                try:
                    run_record = RunTransformer.transform(run)

                    tasks = client.get_tasks(run_record.run_id)

                    transformed_tasks = [
                        TaskTransformer.transform(run_record.run_id, task)
                        for task in tasks
                    ]

                    run_record.tasks = transformed_tasks

                    run_records.append(run_record)

                    task_records.extend(transformed_tasks)

                except ValueError as exc:
                    run_id = run.get("run_id", "unknown")

                    logging.exception(
                        "Run %s could not be transformed. "
                        "The checkpoint will not be advanced.",
                        run_id,
                    )

                    raise RuntimeError(
                        f"Transformation failed for run {run_id}."
                    ) from exc

        for task_record in task_records:
            self.validate_task_record(task_record)

        total_runs = self.runs_repository.save_all(run_records)
        total_tasks = self.tasks_repository.save_all(task_records)

        self.checkpoint_repository.update_processed_until(checkpoint_target)

        logging.info(
            "Databricks loading completed. "
            "Runs saved: %s. "
            "Tasks saved: %s. "
            "Checkpoint updated to: %s.",
            total_runs,
            total_tasks,
            checkpoint_target,
        )

        return total_runs + total_tasks

    def validate_task_record(self, task) -> None:
        duration = task.duration

        if duration is None:
            return

        if isinstance(duration, str):
            raise ValueError(
                "Task duration has an invalid type. "
                f"task_run_id={task.task_run_id}, "
                f"run_id={task.run_id}, "
                f"duration={duration!r}, "
                f"type={type(duration).__name__}"
            )

        if not isinstance(duration, timedelta):
            raise ValueError(
                "Task duration must be timedelta or None. "
                f"task_run_id={task.task_run_id}, "
                f"run_id={task.run_id}, "
                f"duration={duration!r}, "
                f"type={type(duration).__name__}"
            )

        if duration < timedelta(0):
            raise ValueError(
                "Negative task duration. "
                f"task_run_id={task.task_run_id}, "
                f"run_id={task.run_id}, "
                f"duration={duration}"
            )

        if duration > timedelta(days=7):
            raise ValueError(
                "Suspicious task duration. "
                f"task_run_id={task.task_run_id}, "
                f"run_id={task.run_id}, "
                f"started={task.started_cdmx}, "
                f"ended={task.ended_cdmx}, "
                f"duration={duration}"
            )
