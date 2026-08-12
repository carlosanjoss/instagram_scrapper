from collections.abc import Callable

from src.services.instagram.comment import CommentService
from src.services.orchestrator.handlers.common import model_payload
from src.services.orchestrator.task_contract import HandlerResult, Task


class FetchCommentsHandler:
    def __init__(self, comment_service: CommentService, delay: Callable[[str], None]) -> None:
        self.comment_service = comment_service
        self.delay = delay

    def handle(self, task: Task) -> HandlerResult:
        media_id = str(task["media_id"])
        limit = int(task.get("comments_limit", 30))
        print(f"\nTarefa fetch_comments -> {media_id}")
        comments = self.comment_service.fetch_media_comments(media_id, amount=limit)
        self.delay("media_comments")
        print(f"Comentários da mídia {media_id}: {len(comments)}")
        return HandlerResult(
            records=[("instagram.comments.data", media_id, model_payload(comment)) for comment in comments]
        )
