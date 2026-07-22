import random
import time
from collections.abc import Callable

from src.services.orchestrator.task_contract import HandlerResult


class RetryPolicy:
    FATAL_MARKERS = (
        "challengeresolve",
        "unknown step_name",
        "challenge resolver",
        "challenge_required",
        "checkpoint_required",
        "loginrequired",
        "requestsjsondecodeerror",
        "jsondecodeerror",
        "mixins/challenge.py",
    )

    def __init__(self, attempts: int = 3) -> None:
        self.attempts = attempts

    @classmethod
    def is_fatal(cls, exc: Exception) -> bool:
        parts = []
        current: BaseException | None = exc
        while current is not None:
            parts.append(f"{type(current).__name__}: {current}")
            current = current.__cause__ or current.__context__
        error_text = "\n".join(parts).lower()
        return any(marker in error_text for marker in cls.FATAL_MARKERS)

    def execute(self, operation: Callable[[], HandlerResult]) -> HandlerResult:
        for attempt in range(1, self.attempts + 1):
            try:
                return operation()
            except Exception as exc:
                if self.is_fatal(exc) or attempt == self.attempts:
                    raise
                delay = (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"Falha transitória; tentativa {attempt}/{self.attempts}. Retry em {delay:.2f}s: {exc}")
                time.sleep(delay)
        raise RuntimeError("Política de retry finalizada sem resultado")
