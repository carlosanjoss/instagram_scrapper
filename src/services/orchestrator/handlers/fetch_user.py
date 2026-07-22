from collections.abc import Callable
from typing import Any

from src.services.instagram.media import MediaService
from src.services.instagram.user import UserService
from src.services.orchestrator.handlers.common import model_payload
from src.services.orchestrator.snapshot_repository import SnapshotRepository
from src.services.orchestrator.task_contract import HandlerResult, Task, child_task_id


class FetchUserHandler:
    def __init__(
        self,
        user_service: UserService,
        media_service: MediaService,
        snapshots: SnapshotRepository,
        task_topic: str,
        delay: Callable[[str], None],
    ) -> None:
        self.user_service = user_service
        self.media_service = media_service
        self.snapshots = snapshots
        self.task_topic = task_topic
        self.delay = delay

    def handle(self, task: Task) -> HandlerResult:
        username = str(task["username"])
        media_limit = int(task.get("media_limit", 10))
        comments_limit = int(task.get("comments_limit", 30))
        stories_limit = int(task.get("stories_limit", 10))
        root_task_id = str(task["root_task_id"])
        result = HandlerResult()

        print(f"\nTarefa fetch_user -> {username}")
        user_info = self.user_service.get_user_info_by_username(username)
        user_payload = model_payload(user_info)
        user_id = str(user_info.pk)
        previous = self.snapshots.load(SnapshotRepository.USER, user_id)

        if not self.snapshots.profile(previous) or self.snapshots.profile(previous) != self.snapshots.profile(user_payload):
            result.records.append(("instagram.user.data", str(user_info.pk), user_payload))
            self.delay("user_info_by_username")
            if not self.snapshots.profile(previous):
                print(f"Perfil novo enviado para instagram.user.data -> @{username}")
            else:
                print(f"Alteração de perfil detectada e reenviada -> @{username}")
        else:
            print(f"Perfil sem mudanças, não reenviado -> @{username}")

        previous_count = self.snapshots.extract_media_count(previous)
        current_count = self.snapshots.extract_media_count(user_payload)
        snapshot = self.snapshots.timestamped(user_payload)
        record, update = self.snapshots.stage(SnapshotRepository.USER, user_id, snapshot)
        result.records.append(record)
        result.snapshot_updates.append(update)

        story_task = {
            "task_id": child_task_id(root_task_id, "story", user_id),
            "root_task_id": root_task_id,
            "type": "fetch_story",
            "user_id": user_id,
            "stories_limit": stories_limit,
        }
        result.records.append((self.task_topic, user_id, story_task))

        amount = media_limit
        should_fetch = True
        if previous_count is not None and current_count is not None:
            delta = current_count - previous_count
            if delta <= 0:
                print(f"Sem novas mídias para @{username}. Anterior={previous_count}, Atual={current_count}")
                should_fetch = False
            else:
                amount = min(media_limit, delta)
                print(f"Mudança detectada para @{username}. Anterior={previous_count}, Atual={current_count}, novas={delta}, coletando={amount}")
        else:
            print(f"Sem snapshot anterior para @{username}. Coletando baseline de {amount} mídias.")

        if should_fetch:
            medias = self.media_service.get_user_medias(user_id, amount=amount)
            self.delay("user_medias")
            print(f"Mídias encontradas para {username}: {len(medias)}")
            for media in medias:
                media_task: dict[str, Any] = {
                    "task_id": child_task_id(root_task_id, "media", media.pk),
                    "root_task_id": root_task_id,
                    "type": "fetch_media",
                    "user_id": user_id,
                    "media_id": str(media.pk),
                    "comments_limit": comments_limit,
                }
                result.records.append((self.task_topic, user_id, media_task))
        return result
