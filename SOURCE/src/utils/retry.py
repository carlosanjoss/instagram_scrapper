import random
import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")


class RetryPolicy:
    RATE_LIMIT_MARKERS = (
        "429",
        "too many requests",
        "clientthrottlederror",
        "ratelimiterror",
        "pleasewaitfewminutes",
        "please wait a few minutes",
        "feedback_required",
    )
    LOGIN_REQUIRED_MARKERS = (
        "loginrequired",
        "login_required",
    )
    FATAL_MARKERS = (
        *RATE_LIMIT_MARKERS,
        "suspicious login",
        "suspeita de automa",
        "automated behavior",
        "challengeresolve",
        "unknown step_name",
        "challenge resolver",
        "challenge_required",
        "checkpoint_required",
        "loginrequired",
        "mixins/challenge.py",
        "jsondecodeerror",           # Instagram retorna HTML (login/challenge) em vez de JSON
        "clientjsondecodeerror",     # instagrapi wrapper para JSONDecodeError
        "expecting value",           # json.decoder.JSONDecodeError: Expecting value
    )

    def __init__(self, attempts: int = 3) -> None:
        if attempts < 1:
            raise ValueError("A quantidade de tentativas deve ser positiva")
        self.attempts = attempts

    @classmethod
    def is_fatal(cls, exc: Exception) -> bool:
        error_text = cls._error_text(exc)
        return any(marker in error_text for marker in cls.FATAL_MARKERS)

    @staticmethod
    def _error_text(exc: Exception) -> str:
        parts = []
        current: BaseException | None = exc
        while current is not None:
            parts.append(f"{type(current).__name__}: {current}")
            current = current.__cause__ or current.__context__
        return "\n".join(parts).lower()

    @classmethod
    def is_rate_limited(cls, exc: Exception) -> bool:
        error_text = cls._error_text(exc)
        return any(marker in error_text for marker in cls.RATE_LIMIT_MARKERS)

    @classmethod
    def is_login_required(cls, exc: Exception) -> bool:
        error_text = cls._error_text(exc)
        return any(marker in error_text for marker in cls.LOGIN_REQUIRED_MARKERS)

    def execute(self, operation: Callable[[], T]) -> T:
        for attempt in range(1, self.attempts + 1):
            try:
                return operation()
            except Exception as exc:
                if self.is_fatal(exc):
                    print(
                        "Coleta interrompida sem retry por erro de segurança/limite: "
                        f"{exc}"
                    )
                    raise
                if attempt == self.attempts:
                    raise
                delay = (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(
                    f"Falha transitória; tentativa {attempt}/{self.attempts}. "
                    f"Retry em {delay:.2f}s: {exc}"
                )
                time.sleep(delay)
        raise RuntimeError("Política de retry finalizada sem resultado")
