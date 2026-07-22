from datetime import datetime, timezone
from typing import Any, Dict

from src.services.kafka.producer import KafkaService
from src.services.orchestrator.task_contract import KafkaRecord, SnapshotUpdate


class SnapshotRepository:
    USER = "user"
    MEDIA_COMMENTS = "media_comments"
    USER_STORIES = "user_stories"

    def __init__(
        self,
        kafka: KafkaService,
        user_topic: str,
        media_comments_topic: str,
        user_stories_topic: str,
    ) -> None:
        self.kafka = kafka
        self.topics = {
            self.USER: user_topic,
            self.MEDIA_COMMENTS: media_comments_topic,
            self.USER_STORIES: user_stories_topic,
        }
        self.caches: Dict[str, Dict[str, Dict[str, Any]]] = {
            namespace: {} for namespace in self.topics
        }

    def load(self, namespace: str, key: str) -> Dict[str, Any] | None:
        cache = self.caches[namespace]
        if key not in cache:
            value = self.kafka.get_latest_by_key(self.topics[namespace], key)
            if isinstance(value, dict):
                cache[key] = value
        return cache.get(key)

    def stage(self, namespace: str, key: str, value: Dict[str, Any]) -> tuple[KafkaRecord, SnapshotUpdate]:
        record = (self.topics[namespace], key, value)
        return record, SnapshotUpdate(namespace, key, value)

    def commit(self, updates: list[SnapshotUpdate]) -> None:
        for update in updates:
            self.caches[update.namespace][update.key] = update.value

    @staticmethod
    def timestamped(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {**payload, "snapshot_at": datetime.now(timezone.utc).isoformat()}

    @staticmethod
    def extract_count(snapshot: Dict[str, Any] | None, fields: tuple[str, ...]) -> int | None:
        if not snapshot:
            return None
        for field_name in fields:
            value = snapshot.get(field_name)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue
        return None

    @staticmethod
    def extract_media_count(snapshot: Dict[str, Any] | None) -> int | None:
        direct = SnapshotRepository.extract_count(snapshot, ("media_count", "total_medias", "medias_count"))
        if direct is not None or not snapshot:
            return direct
        edge = snapshot.get("edge_owner_to_timeline_media")
        return SnapshotRepository.extract_count(edge, ("count",)) if isinstance(edge, dict) else None

    @staticmethod
    def extract_comments_count(snapshot: Dict[str, Any] | None) -> int | None:
        direct = SnapshotRepository.extract_count(snapshot, ("comment_count", "comments_count", "comments"))
        if direct is not None or not snapshot:
            return direct
        preview = snapshot.get("preview_comments")
        return len(preview) if isinstance(preview, list) else None

    @staticmethod
    def profile(snapshot: Dict[str, Any] | None) -> Dict[str, Any]:
        return {key: value for key, value in (snapshot or {}).items() if key != "snapshot_at"}
