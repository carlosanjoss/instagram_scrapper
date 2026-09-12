import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonRepository:
    """Persiste a coleta em uma hierarquia de arquivos JSON por entidade."""

    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)
        self.users_dir = self.output_dir / "users"

    @staticmethod
    def _entity_id(payload: dict[str, Any], *fields: str) -> str:
        for field in fields:
            value = payload.get(field)
            if value not in (None, ""):
                return str(value)
        raise ValueError(f"Registro sem identificador ({', '.join(fields)})")

    @staticmethod
    def _json_default(value: Any) -> str:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat()
        return str(value)

    def read_json(self, path: Path, default: Any = None) -> Any:
        if not path.exists() or path.stat().st_size == 0:
            return default
        try:
            with path.open("r", encoding="utf-8") as file_obj:
                return json.load(file_obj)
        except (json.JSONDecodeError, OSError):
            return default

    def write_json(self, path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as file_obj:
                json.dump(
                    data,
                    file_obj,
                    ensure_ascii=False,
                    indent=2,
                    default=self._json_default,
                )
                file_obj.write("\n")
                file_obj.flush()
                os.fsync(file_obj.fileno())
            os.replace(temporary_name, path)
        except Exception:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
            raise

    def user_dir(self, user_id: str) -> Path:
        return self.users_dir / str(user_id)

    def media_dir(self, user_id: str, media_id: str) -> Path:
        return self.user_dir(user_id) / "media" / str(media_id)

    def user_path(self, user_id: str) -> Path:
        return self.user_dir(user_id) / "user.json"

    def media_path(self, user_id: str, media_id: str) -> Path:
        return self.media_dir(user_id, media_id) / "media.json"

    def comments_path(self, user_id: str, media_id: str) -> Path:
        return self.media_dir(user_id, media_id) / "comments.json"

    def story_path(self, user_id: str, story_id: str) -> Path:
        return self.user_dir(user_id) / "stories" / f"{story_id}.json"

    def load_user(self, user_id: str) -> dict[str, Any] | None:
        value = self.read_json(self.user_path(user_id))
        return value if isinstance(value, dict) else None

    def load_media(self, user_id: str, media_id: str) -> dict[str, Any] | None:
        value = self.read_json(self.media_path(user_id, media_id))
        return value if isinstance(value, dict) else None

    def save_user(self, payload: dict[str, Any]) -> Path:
        user_id = self._entity_id(payload, "pk", "id", "user_id")
        data = {**payload, "collected_at": utc_now_iso()}
        path = self.user_path(user_id)
        self.write_json(path, data)
        return path

    def save_media(self, user_id: str, payload: dict[str, Any]) -> Path:
        media_id = self._entity_id(payload, "pk", "id", "media_id")
        data = {**payload, "collected_at": utc_now_iso()}
        path = self.media_path(user_id, media_id)
        self.write_json(path, data)
        return path

    def save_story(self, user_id: str, payload: dict[str, Any]) -> tuple[Path, bool]:
        story_id = self._entity_id(payload, "pk", "id", "story_id")
        path = self.story_path(user_id, story_id)
        if path.exists():
            return path, False
        self.write_json(path, {**payload, "collected_at": utc_now_iso()})
        return path, True

    def save_comments(
        self,
        user_id: str,
        media_id: str,
        comments: Iterable[dict[str, Any]],
    ) -> tuple[Path, int]:
        path = self.comments_path(user_id, media_id)
        current = self.read_json(path, [])
        if not isinstance(current, list):
            current = []

        merged: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for item in current:
            if not isinstance(item, dict):
                continue
            try:
                identifier = self._entity_id(item, "pk", "id", "comment_id")
            except ValueError:
                continue
            if identifier not in merged:
                order.append(identifier)
            merged[identifier] = item

        added = 0
        collected_at = utc_now_iso()
        for item in comments:
            identifier = self._entity_id(item, "pk", "id", "comment_id")
            if identifier not in merged:
                order.append(identifier)
                added += 1
            merged[identifier] = {**item, "collected_at": collected_at}

        self.write_json(path, [merged[identifier] for identifier in order])
        return path, added

    def append_observation(self, path: Path, observation: dict[str, Any]) -> None:
        observations = self.read_json(path, [])
        if not isinstance(observations, list):
            observations = []
        observations.append(observation)
        self.write_json(path, observations)

    def save_user_observation(self, user_id: str, observation: dict[str, Any]) -> Path:
        path = self.user_dir(user_id) / "observations.json"
        self.append_observation(path, observation)
        return path

    def save_media_observation(
        self,
        user_id: str,
        media_id: str,
        observation: dict[str, Any],
    ) -> Path:
        path = self.media_dir(user_id, media_id) / "observations.json"
        self.append_observation(path, observation)
        return path
