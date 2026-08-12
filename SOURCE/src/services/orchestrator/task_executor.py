from src.services.orchestrator.retry_policy import RetryPolicy
from src.services.orchestrator.task_contract import (
    ExecutionResult,
    Task,
    TaskHandler,
    TaskValidationError,
    validate_task,
)


class TaskExecutor:
    def __init__(
        self,
        handlers: dict[str, TaskHandler],
        retry_policy: RetryPolicy,
    ) -> None:
        self.handlers = handlers
        self.retry_policy = retry_policy

    def execute(self, task: Task) -> ExecutionResult:
        try:
            task_type = validate_task(task, set(self.handlers))
        except TaskValidationError as exc:
            return ExecutionResult(status="failed", error=exc)

        try:
            result = self.retry_policy.execute(lambda: self.handlers[task_type].handle(task))
            return ExecutionResult(status="completed", handler_result=result)
        except Exception as exc:
            status = "fatal" if self.retry_policy.is_fatal(exc) else "failed"
            return ExecutionResult(status=status, error=exc)
