import random
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from instagrapi import Client

from src.services.instagram.comment import CommentService
from src.services.instagram.media import MediaService
from src.services.instagram.story import StoryService
from src.services.instagram.user import UserService
from src.utils.collection import (
    first_value,
    media_observation,
    model_payload,
)
from src.services.storage import JsonRepository
from src.utils.retry import RetryPolicy


class InstagramJsonCollector:
    """Coleta perfis diretamente e salva cada entidade em JSON local."""

    def __init__(
        self,
        client: Client,
        repository: JsonRepository,
        min_delay_seconds: float = 8.0,
        max_delay_seconds: float = 40.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if min_delay_seconds < 0 or max_delay_seconds < min_delay_seconds:
            raise ValueError("Intervalo de pausa inválido")
        self.repository = repository
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.sleep = sleep
        self.retry = RetryPolicy(attempts=3)
        self.users = UserService(client)
        self.media = MediaService(client)
        self.comments = CommentService(client)
        self.stories = StoryService(client)

    def _delay(self, context: str) -> None:
        wait = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        print(f"Pausa ({context}) por {wait:.2f}s para reduzir padrão automatizado")
        self.sleep(wait)

    def _call(self, operation: Callable[[], Any], context: str) -> Any:
        value = self.retry.execute(operation)
        self._delay(context)
        return value

    @staticmethod
    def _count(payload: dict[str, Any] | None, fields: tuple[str, ...]) -> int | None:
        if not payload:
            return None
        for field in fields:
            value = payload.get(field)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    continue
        return None

    @classmethod
    def _media_count(cls, payload: dict[str, Any] | None) -> int | None:
        direct = cls._count(payload, ("media_count", "total_medias", "medias_count"))
        if direct is not None or not payload:
            return direct
        edge = payload.get("edge_owner_to_timeline_media")
        return cls._count(edge, ("count",)) if isinstance(edge, dict) else None

    @staticmethod
    def _timestamped(payload: dict[str, Any]) -> dict[str, Any]:
        return {**payload, "snapshot_at": datetime.now(timezone.utc).isoformat()}

    def collect_user(
        self,
        username: str,
        media_limit: int = 10,
        stories_limit: int = 10,
        comments_limit: int = 30,
    ) -> dict[str, int | str]:
        print(f"\nColetando @{username}")
        user = self._call(
            lambda: self.users.get_user_info_by_username(username),
            "user_info_by_username",
        )
        user_payload = model_payload(user)
        raw_user_id = first_value(user_payload, "pk", "id", "user_id")
        if raw_user_id is None:
            raise ValueError(f"Perfil @{username} retornado sem identificador")
        user_id = str(raw_user_id)
        previous_user = self.repository.load_user(user_id)
        followers_count = self._count(
            user_payload,
            ("follower_count", "followers_count"),
        )

        self.repository.save_user(user_payload)
        self.repository.save_user_observation(
            user_id,
            self._timestamped(
                {
                    "schema_version": 1,
                    "user_id": user_id,
                    "username": username,
                    "followers_count": followers_count,
                    "following_count": first_value(user_payload, "following_count"),
                    "media_count": self._media_count(user_payload),
                    "is_private": user_payload.get("is_private"),
                }
            ),
        )

        stories = (
            self._call(
                lambda: self.stories.get_user_stories(user_id, amount=stories_limit),
                "user_stories",
            )
            if stories_limit > 0
            else []
        )
        new_stories = 0
        for story in stories:
            _, created = self.repository.save_story(user_id, model_payload(story))
            new_stories += int(created)

        medias = (
            self._call(
                lambda: self.media.get_user_medias(user_id, amount=media_limit),
                "user_medias",
            )
            if media_limit > 0
            else []
        )
        comments_added = 0
        for media_stub in medias:
            raw_media_id = first_value(model_payload(media_stub), "pk", "id", "media_id")
            if raw_media_id is None:
                print(f"Mídia sem identificador ignorada para @{username}")
                continue
            media_id = str(raw_media_id)
            previous_media = self.repository.load_media(user_id, media_id)
            media_model = self._call(
                lambda media_id=media_id: self.media.get_media_info(media_id),
                "media_info",
            )
            payload = model_payload(media_model)
            current_count = self._count(payload, ("comment_count", "comments_count"))
            previous_count = self._count(previous_media, ("comment_count", "comments_count"))

            amount = comments_limit
            comments_file_exists = self.repository.comments_path(user_id, media_id).exists()
            should_fetch_comments = comments_limit > 0 and current_count != 0
            if previous_count is not None and current_count is not None:
                delta = current_count - previous_count
                if delta <= 0 and comments_file_exists:
                    print(f"Sem novos comentários na mídia {media_id}")
                    should_fetch_comments = False
                elif delta > 0:
                    amount = min(comments_limit, delta)

            if should_fetch_comments:
                collected = self._call(
                    lambda media_id=media_id, amount=amount: self.comments.fetch_media_comments(
                        media_id,
                        amount=amount,
                    ),
                    "media_comments",
                )
                _, added = self.repository.save_comments(
                    user_id,
                    media_id,
                    (model_payload(comment) for comment in collected),
                )
                comments_added += added

            self.repository.save_media(user_id, payload)
            self.repository.save_media_observation(
                user_id,
                media_id,
                media_observation(payload, user_id, username, followers_count),
            )

        previous_media_count = self._media_count(previous_user)
        current_media_count = self._media_count(user_payload)
        if previous_media_count is not None and current_media_count is not None:
            print(
                f"@{username}: mídias anterior={previous_media_count}, "
                f"atual={current_media_count}"
            )
        print(
            f"@{username}: {len(medias)} mídias observadas, "
            f"{comments_added} comentários novos e {new_stories} stories novos"
        )
        return {
            "username": username,
            "user_id": user_id,
            "media_observed": len(medias),
            "comments_added": comments_added,
            "stories_added": new_stories,
        }
