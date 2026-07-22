import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Protocol, Tuple


Task = Dict[str, Any]
KafkaRecord = Tuple[str, str | None, Dict[str, Any]]


class TaskValidationError(ValueError):
    pass


@dataclass(frozen=True)
class SnapshotUpdate:
    namespace: str
    key: str
    value: Dict[str, Any]


@dataclass
class HandlerResult:
    records: list[KafkaRecord] = field(default_factory=list)
    snapshot_updates: list[SnapshotUpdate] = field(default_factory=list)

    def extend(self, other: "HandlerResult") -> None:
        self.records.extend(other.records)
        self.snapshot_updates.extend(other.snapshot_updates)


@dataclass
class ExecutionResult:
    status: str
    handler_result: HandlerResult = field(default_factory=HandlerResult)
    error: Exception | None = None


class TaskHandler(Protocol):
    def handle(self, task: Task) -> HandlerResult: ...


REQUIRED_FIELDS = {
    "fetch_user": ("username",),
    "fetch_media": ("user_id", "media_id"),
    "fetch_story": ("user_id",),
    "fetch_comments": ("media_id",),
}


def validate_task(task: Task, known_types: set[str]) -> str:
    if not isinstance(task, dict):
        raise TaskValidationError("A tarefa deve ser um objeto JSON")
    for field_name in ("task_id", "root_task_id", "type"):
        if not task.get(field_name):
            raise TaskValidationError(f"Campo obrigatório ausente: {field_name}")
    task_type = str(task["type"])
    if task_type not in known_types or task_type not in REQUIRED_FIELDS:
        raise TaskValidationError(f"Tipo de tarefa desconhecido: {task_type}")
    for field_name in REQUIRED_FIELDS[task_type]:
        if task.get(field_name) in (None, ""):
            raise TaskValidationError(f"Campo obrigatório para {task_type}: {field_name}")
    return task_type


def create_fetch_user_task(username: str, **limits: Any) -> Task:
    root_task_id = str(uuid.uuid4())
    return {
        "task_id": root_task_id,
        "root_task_id": root_task_id,
        "type": "fetch_user",
        "username": username,
        **limits,
    }


def child_task_id(root_task_id: str, kind: str, entity_id: Any) -> str:
    return f"{root_task_id}:{kind}:{entity_id}"
