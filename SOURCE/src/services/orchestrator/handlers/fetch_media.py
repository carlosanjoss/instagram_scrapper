from collections.abc import Callable

from src.services.instagram.media import MediaService
from src.services.orchestrator.handlers.common import media_observation, model_payload
from src.services.orchestrator.snapshot_repository import SnapshotRepository
from src.services.orchestrator.task_contract import HandlerResult, Task, child_task_id


class FetchMediaHandler:
    def __init__(self, media_service: MediaService, snapshots: SnapshotRepository, task_topic: str, media_observation_topic: str, delay: Callable[[str], None]) -> None:
        self.media_service = media_service
        self.snapshots = snapshots
        self.task_topic = task_topic
        self.media_observation_topic = media_observation_topic
        self.delay = delay

    def handle(self, task: Task) -> HandlerResult:
        user_id = str(task["user_id"])
        media_id = str(task["media_id"])
        limit = int(task.get("comments_limit", 30))
        root_task_id = str(task["root_task_id"])
        result = HandlerResult()
        print(f"\nTarefa fetch_media -> {media_id}")

        media = self.media_service.get_media_info(media_id)
        payload = model_payload(media)
        result.records.append(("instagram.media.data", user_id, payload))
        result.records.append((self.media_observation_topic, media_id, media_observation(
            payload, user_id,
            str(task["username"]) if task.get("username") else None,
            int(task["followers_count"]) if task.get("followers_count") is not None else None,
        )))
        self.delay("media_info")

        previous = self.snapshots.load(SnapshotRepository.MEDIA_COMMENTS, media_id)
        previous_count = self.snapshots.extract_comments_count(previous)
        current_count = self.snapshots.extract_comments_count(payload)
        amount = limit
        should_fetch = True
        if previous_count is not None and current_count is not None:
            delta = current_count - previous_count
            if delta <= 0:
                print(f"Sem novos comentários na mídia {media_id}. Anterior={previous_count}, Atual={current_count}")
                should_fetch = False
            else:
                amount = min(limit, delta)
                print(f"Novos comentários detectados na mídia {media_id}. Anterior={previous_count}, Atual={current_count}, novos={delta}, coletando={amount}")
        else:
            print(f"Sem snapshot de comentários para mídia {media_id}. Coletando baseline de {amount} comentários.")

        snapshot = self.snapshots.timestamped({"comment_count": current_count})
        record, update = self.snapshots.stage(SnapshotRepository.MEDIA_COMMENTS, media_id, snapshot)
        result.records.append(record)
        result.snapshot_updates.append(update)
        if should_fetch:
            comments_task = {
                "task_id": child_task_id(root_task_id, "comments", media_id),
                "root_task_id": root_task_id,
                "type": "fetch_comments",
                "media_id": media_id,
                "comments_limit": amount,
            }
            result.records.append((self.task_topic, media_id, comments_task))
        return result
