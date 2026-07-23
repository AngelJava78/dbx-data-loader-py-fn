from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

@dataclass(frozen=True)
class TaskRecord:
    task_run_id: int | None
    run_id: int
    task_key: str | None
    started_cdmx: datetime | None
    ended_cdmx: datetime | None
    duration: timedelta | None
    task_type: str | None
    run_page_url: str | None
    status: str | None
    notebook_path: str | None
    notebook_name: str | None
    process_id: int | None
    subprocess_id: int | None
    stage_id: int | None
    substage_id: int | None
    username: str | None
    folio_number: str | None
    parameter_source: str | None
 
    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.task_run_id,
            self.run_id,
            self.task_key,
            self.started_cdmx,
            self.ended_cdmx,
            self.duration,
            self.task_type,
            self.run_page_url,
            self.status,
            self.notebook_path,
            self.notebook_name,
            self.process_id,
            self.subprocess_id,
            self.stage_id,
            self.substage_id,
            self.username,
            self.folio_number,
            self.parameter_source,
        )
