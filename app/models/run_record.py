from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from app.models.task_record import TaskRecord

@dataclass
class RunRecord:
    run_id: int
    job_id: int | None
    job_name: str | None
    started_cdmx: datetime | None
    ended_cdmx: datetime | None
    duration: timedelta | None
    run_type: str | None
    result_state: str | None
    termination_code: str | None
    workspace_id: int | None
    run_page_url: str | None
    process_id: int | None
    subprocess_id: int | None
    stage_id: int | None
    substage_id: int | None
    username: str
    folio_number: str
    parameter_source: str
    tasks: list[TaskRecord] = field(default_factory=list)

    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.run_id,
            self.job_id,
            self.job_name,
            self.started_cdmx,
            self.ended_cdmx,
            self.duration,
            self.run_type,
            self.result_state,
            self.termination_code,
            self.workspace_id,
            self.run_page_url,
            self.process_id,
            self.subprocess_id,
            self.stage_id,
            self.substage_id,
            self.username,
            self.folio_number,
            self.parameter_source,
        )
