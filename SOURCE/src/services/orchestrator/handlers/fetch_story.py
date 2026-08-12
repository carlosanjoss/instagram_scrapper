from collections.abc import Callable

from src.services.instagram.story import StoryService
from src.services.orchestrator.handlers.common import model_payload, story_id
from src.services.orchestrator.snapshot_repository import SnapshotRepository
from src.services.orchestrator.task_contract import HandlerResult, Task


class FetchStoryHandler:
    def __init__(self, story_service: StoryService, snapshots: SnapshotRepository, delay: Callable[[str], None]) -> None:
        self.story_service = story_service
        self.snapshots = snapshots
        self.delay = delay

    def handle(self, task: Task) -> HandlerResult:
        user_id = str(task["user_id"])
        limit = int(task.get("stories_limit", 10))
        result = HandlerResult()
        print(f"\nTarefa fetch_story -> user {user_id}")

        stories = self.story_service.get_user_stories(user_id, amount=limit)
        previous = self.snapshots.load(SnapshotRepository.USER_STORIES, user_id)
        previous_ids = {str(item) for item in (previous or {}).get("story_ids", [])}
        identified = [(identifier, story) for story in stories if (identifier := story_id(story)) is not None]
        current_ids = [identifier for identifier, _ in identified]
        new_stories = [story for identifier, story in identified if identifier not in previous_ids]

        snapshot = self.snapshots.timestamped({"stories_count": len(current_ids), "story_ids": current_ids})
        record, update = self.snapshots.stage(SnapshotRepository.USER_STORIES, user_id, snapshot)
        result.records.append(record)
        result.snapshot_updates.append(update)
        self.delay("user_stories")
        print(f"Stories encontradas para user {user_id}: {len(identified)}")

        previous_count = None
        if previous is not None:
            try:
                previous_count = int(previous.get("stories_count"))
            except (TypeError, ValueError):
                previous_count = len(previous_ids)
        if previous_count is None:
            print(f"Sem snapshot de stories para user {user_id}. Enviando baseline de {len(new_stories)} stories.")
        else:
            print(f"Stories user {user_id}: anterior={previous_count}, atual={len(current_ids)}, novas={len(new_stories)}")

        result.records.extend(
            ("instagram.stories.data", user_id, model_payload(story)) for story in new_stories
        )
        return result
