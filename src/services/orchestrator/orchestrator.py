import argparse
import random
import traceback
import time
from threading import Event
from datetime import datetime, timezone
from typing import Any, Dict

from dotenv import find_dotenv, load_dotenv
from instagrapi import Client

from src.auth.autenticacao import login_with_persistence
from src.services.instagram.comment import CommentService
from src.services.instagram.media import MediaService
from src.services.instagram.story import StoryService
from src.services.instagram.user import UserService
from src.services.kafka.consumer import KafkaTaskConsumer
from src.services.kafka.producer import KafkaService


class InstagramKafkaOrchestrator:
    """Consumidor de tarefas Kafka para coleta Instagram com delays anti-detecção."""

    def __init__(
        self,
        client: Client,
        task_topic: str = "instagram.tasks",
        user_state_topic: str = "instagram.user.latest",
        media_comments_state_topic: str = "instagram.media.comments.latest",
        user_stories_state_topic: str = "instagram.user.stories.latest",
        bootstrap_servers: str = "localhost:29092",
        consumer_group: str = "instagram-orchestrator-workers",
        min_delay_seconds: float = 8.0,
        max_delay_seconds: float = 40.0,
    ) -> None:
        self.cl = client
        self.kafka = KafkaService(bootstrap_servers=bootstrap_servers)
        self.user_service = UserService(client)
        self.media_service = MediaService(client)
        self.comment_service = CommentService(client)
        self.story_service = StoryService(client)
        self.task_topic = task_topic
        self.user_state_topic = user_state_topic
        self.media_comments_state_topic = media_comments_state_topic
        self.user_stories_state_topic = user_stories_state_topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self._user_state_cache: Dict[str, Dict[str, Any]] = {}
        self._media_comments_state_cache: Dict[str, Dict[str, Any]] = {}
        self._user_stories_state_cache: Dict[str, Dict[str, Any]] = {}

    def _human_delay(self, context: str) -> None:
        wait = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        print(f"⏳ Pausa ({context}) por {wait:.2f}s para reduzir padrão automatizado")
        time.sleep(wait)

    def _send_user(self, user_info) -> None:
        self.kafka.send_to_topic(
            topic="instagram.user.data",
            key=user_info.pk,
            data=user_info.dict(),
        )

    def _send_media(self, user_id: str, media) -> None:
        # Regra solicitada: key da mídia = id do usuário
        self.kafka.send_to_topic(
            topic="instagram.media.data",
            key=user_id,
            data=media.dict(),
        )

    def _send_comment(self, media_id: str, comment) -> None:
        # Regra solicitada: key do comentário = id da mídia
        self.kafka.send_to_topic(
            topic="instagram.comments.data",
            key=media_id,
            data=comment.dict(),
        )

    def _send_story(self, user_id: str, story) -> None:
        # Regra solicitada: key da história = id do user
        self.kafka.send_to_topic(
            topic="instagram.stories.data",
            key=user_id,
            data=story.dict(),
        )

    def _enqueue_task(self, task: Dict[str, Any], key: str = "") -> None:
        self.kafka.send_to_topic(
            topic=self.task_topic,
            key=key or task.get("username") or task.get("media_id") or "task",
            data=task,
        )

    @staticmethod
    def _extract_media_count(snapshot: Dict[str, Any] | None) -> int | None:
        if not snapshot:
            return None

        for field in ("media_count", "total_medias", "medias_count"):
            value = snapshot.get(field)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass

        edge = snapshot.get("edge_owner_to_timeline_media")
        if isinstance(edge, dict):
            value = edge.get("count")
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return None

        return None

    def _load_previous_user_snapshot(self, user_id: str) -> Dict[str, Any] | None:
        if user_id in self._user_state_cache:
            return self._user_state_cache[user_id]

        latest = self.kafka.get_latest_by_key(topic=self.user_state_topic, key=user_id)
        if isinstance(latest, dict):
            self._user_state_cache[user_id] = latest
            return latest

        return None

    def _save_user_snapshot(self, user_id: str, user_payload: Dict[str, Any]) -> None:
        snapshot = {
            **user_payload,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
        }
        self.kafka.send_to_topic(
            topic=self.user_state_topic,
            key=user_id,
            data=snapshot,
        )
        self._user_state_cache[user_id] = snapshot

    @staticmethod
    def _extract_comments_count(snapshot: Dict[str, Any] | None) -> int | None:
        if not snapshot:
            return None

        for field in ("comment_count", "comments_count", "comments"):
            value = snapshot.get(field)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass

        preview_comments = snapshot.get("preview_comments")
        if isinstance(preview_comments, list):
            return len(preview_comments)

        return None

    def _load_previous_media_comments_snapshot(self, media_id: str) -> Dict[str, Any] | None:
        if media_id in self._media_comments_state_cache:
            return self._media_comments_state_cache[media_id]

        latest = self.kafka.get_latest_by_key(topic=self.media_comments_state_topic, key=media_id)
        if isinstance(latest, dict):
            self._media_comments_state_cache[media_id] = latest
            return latest

        return None

    def _save_media_comments_snapshot(self, media_id: str, comments_count: int | None) -> None:
        snapshot = {
            "comment_count": comments_count,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
        }
        self.kafka.send_to_topic(
            topic=self.media_comments_state_topic,
            key=media_id,
            data=snapshot,
        )
        self._media_comments_state_cache[media_id] = snapshot

    def _load_previous_user_stories_snapshot(self, user_id: str) -> Dict[str, Any] | None:
        if user_id in self._user_stories_state_cache:
            return self._user_stories_state_cache[user_id]

        latest = self.kafka.get_latest_by_key(topic=self.user_stories_state_topic, key=user_id)
        if isinstance(latest, dict):
            self._user_stories_state_cache[user_id] = latest
            return latest

        return None

    def _save_user_stories_snapshot(self, user_id: str, story_ids: list[str]) -> None:
        snapshot = {
            "stories_count": len(story_ids),
            "story_ids": story_ids,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
        }
        self.kafka.send_to_topic(
            topic=self.user_stories_state_topic,
            key=user_id,
            data=snapshot,
        )
        self._user_stories_state_cache[user_id] = snapshot

    @staticmethod
    def _extract_story_id(story) -> str | None:
        for attr in ("pk", "id"):
            value = getattr(story, attr, None)
            if value is not None:
                return str(value)

        if hasattr(story, "dict"):
            payload = story.dict()
            for field in ("pk", "id"):
                value = payload.get(field)
                if value is not None:
                    return str(value)

        return None

    @staticmethod
    def _normalize_snapshot_for_compare(snapshot: Dict[str, Any] | None) -> Dict[str, Any]:
        if not snapshot:
            return {}

        # Remove metadados de controle para comparar somente atributos do perfil.
        return {
            key: value
            for key, value in snapshot.items()
            if key not in {"snapshot_at"}
        }

    def _process_fetch_user(self, task: Dict[str, Any]) -> None:
        username = str(task["username"])
        media_limit = int(task.get("media_limit", 10))
        comments_limit = int(task.get("comments_limit", 30))
        stories_limit = int(task.get("stories_limit", 10))

        print(f"\n👤 Tarefa fetch_user -> {username}")
        user_info = self.user_service.get_user_info_by_username(username)
        user_payload = user_info.dict()

        user_id = str(user_info.pk)
        previous_snapshot = self._load_previous_user_snapshot(user_id)
        previous_profile = self._normalize_snapshot_for_compare(previous_snapshot)
        current_profile = self._normalize_snapshot_for_compare(user_payload)

        if not previous_profile or previous_profile != current_profile:
            self._send_user(user_info)
            self._human_delay("user_info_by_username")
            if not previous_profile:
                print(f"🆕 Perfil novo enviado para instagram.user.data -> @{username}")
            else:
                print(f"🔁 Alteração de perfil detectada e reenviada -> @{username}")
        else:
            print(f"ℹ️ Perfil sem mudanças, não reenviado -> @{username}")

        previous_media_count = self._extract_media_count(previous_snapshot)
        current_media_count = self._extract_media_count(user_payload)

        self._save_user_snapshot(user_id=user_id, user_payload=user_payload)

        # Após publicar user, agenda imediatamente a coleta de stories desse usuário.
        self._enqueue_task(
            {
                "type": "fetch_story",
                "user_id": user_id,
                "stories_limit": stories_limit,
            },
            key=user_id,
        )

        # Decidir se busca novas mídias com base no delta de media_count
        should_fetch_media = True
        media_amount_to_fetch = media_limit
        
        if previous_media_count is not None and current_media_count is not None:
            delta = current_media_count - previous_media_count
            if delta <= 0:
                print(
                    f"ℹ️ Sem novas mídias para @{username}. "
                    f"Anterior={previous_media_count}, Atual={current_media_count}"
                )
                should_fetch_media = False
            else:
                media_amount_to_fetch = min(media_limit, delta)
                print(
                    f"🔄 Mudança detectada para @{username}. "
                    f"Anterior={previous_media_count}, Atual={current_media_count}, "
                    f"novas={delta}, coletando={media_amount_to_fetch}"
                )
        else:
            print(
                f"🆕 Sem snapshot anterior para @{username}. "
                f"Coletando baseline de {media_amount_to_fetch} mídias."
            )

        # Enfileira tarefas de mídia apenas se houver mudança
        if should_fetch_media:
            medias = self.media_service.get_user_medias(user_id, amount=media_amount_to_fetch)
            self._human_delay("user_medias")
            print(f"🧩 Mídias encontradas para {username}: {len(medias)}")

            for media in medias:
                self._enqueue_task(
                    {
                        "type": "fetch_media",
                        "user_id": user_id,
                        "media_id": str(media.pk),
                        "comments_limit": comments_limit,
                    },
                    key=user_id,
                )

    def _process_fetch_story(self, task: Dict[str, Any]) -> None:
        user_id = str(task["user_id"])
        stories_limit = int(task.get("stories_limit", 10))

        print(f"\n📖 Tarefa fetch_story -> user {user_id}")
        stories = self.story_service.get_user_stories(user_id, amount=stories_limit)
        previous_snapshot = self._load_previous_user_stories_snapshot(user_id)
        previous_story_ids = set(str(item) for item in (previous_snapshot or {}).get("story_ids", []))

        stories_with_ids = []
        for story in stories:
            story_id = self._extract_story_id(story)
            if story_id is not None:
                stories_with_ids.append((story_id, story))

        current_story_ids = [story_id for story_id, _ in stories_with_ids]
        new_stories = [story for story_id, story in stories_with_ids if story_id not in previous_story_ids]

        current_count = len(current_story_ids)
        previous_count = None
        if previous_snapshot is not None:
            try:
                previous_count = int(previous_snapshot.get("stories_count"))
            except (TypeError, ValueError):
                previous_count = len(previous_story_ids)

        self._save_user_stories_snapshot(user_id=user_id, story_ids=current_story_ids)
        self._human_delay("user_stories")
        print(f"📖 Stories encontradas para user {user_id}: {len(stories_with_ids)}")

        if previous_count is None:
            print(f"🆕 Sem snapshot de stories para user {user_id}. Enviando baseline de {len(new_stories)} stories.")
        else:
            print(
                f"🔎 Stories user {user_id}: anterior={previous_count}, atual={current_count}, "
                f"novas={len(new_stories)}"
            )

        for story in new_stories:
            self._send_story(user_id=user_id, story=story)

    def _process_fetch_media(self, task: Dict[str, Any]) -> None:
        user_id = str(task["user_id"])
        media_id = str(task["media_id"])
        comments_limit = int(task.get("comments_limit", 30))

        print(f"\n🖼️ Tarefa fetch_media -> {media_id}")
        media = self.media_service.get_media_info(media_id)
        self._send_media(user_id=user_id, media=media)
        self._human_delay("media_info")

        media_payload = media.dict()
        previous_comments_snapshot = self._load_previous_media_comments_snapshot(media_id)
        previous_comments_count = self._extract_comments_count(previous_comments_snapshot)
        current_comments_count = self._extract_comments_count(media_payload)

        should_fetch_comments = True
        comments_amount_to_fetch = comments_limit
        if previous_comments_count is not None and current_comments_count is not None:
            comments_delta = current_comments_count - previous_comments_count
            if comments_delta <= 0:
                print(
                    f"ℹ️ Sem novos comentários na mídia {media_id}. "
                    f"Anterior={previous_comments_count}, Atual={current_comments_count}"
                )
                should_fetch_comments = False
            else:
                comments_amount_to_fetch = min(comments_limit, comments_delta)
                print(
                    f"🔄 Novos comentários detectados na mídia {media_id}. "
                    f"Anterior={previous_comments_count}, Atual={current_comments_count}, "
                    f"novos={comments_delta}, coletando={comments_amount_to_fetch}"
                )
        else:
            print(
                f"🆕 Sem snapshot de comentários para mídia {media_id}. "
                f"Coletando baseline de {comments_amount_to_fetch} comentários."
            )

        self._save_media_comments_snapshot(media_id=media_id, comments_count=current_comments_count)

        if not should_fetch_comments:
            return

        self._enqueue_task(
            {
                "type": "fetch_comments",
                "media_id": media_id,
                "comments_limit": comments_amount_to_fetch,
            },
            key=media_id,
        )

    def _process_fetch_comments(self, task: Dict[str, Any]) -> None:
        media_id = str(task["media_id"])
        comments_limit = int(task.get("comments_limit", 30))

        print(f"\n💬 Tarefa fetch_comments -> {media_id}")
        comments = self.comment_service.fetch_media_comments(media_id, amount=comments_limit)
        self._human_delay("media_comments")
        print(f"💬 Comentários da mídia {media_id}: {len(comments)}")

        for comment in comments:
            self._send_comment(media_id=media_id, comment=comment)

    def process_task(self, task: Dict[str, Any]) -> None:
        task_type = task.get("type")
        if task_type == "fetch_user":
            self._process_fetch_user(task)
            return
        if task_type == "fetch_media":
            self._process_fetch_media(task)
            return
        if task_type == "fetch_story":
            self._process_fetch_story(task)
            return
        if task_type == "fetch_comments":
            self._process_fetch_comments(task)
            return

        print(f"⚠️ Tipo de tarefa desconhecido: {task_type}")

    def _is_fatal_instagram_disconnect(self, exc: Exception) -> bool:
        chain_parts = []
        current = exc
        while current is not None:
            chain_parts.append(f"{type(current).__name__}: {current}")
            current = current.__cause__ or current.__context__

        error_text = "\n".join(chain_parts).lower()

        fatal_markers = [
            "challengeresolve",
            "unknown step_name",
            "challenge resolver",
            "challenge_required",
            "checkpoint_required",
            "loginrequired",
            "requestsjsondecodeerror",
            "jsondecodeerror",
            "mixins/challenge.py",
        ]
        return any(marker in error_text for marker in fatal_markers)

    def _handle_task(self, task: Dict[str, Any]) -> bool:
        try:
            self.process_task(task)
        except Exception as exc:
            full_trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            print(f"❌ Erro ao processar tarefa {task}: {exc}")
            if self._is_fatal_instagram_disconnect(exc):
                print("🛑 Sessão do Instagram inválida/desconectada. Parando consumo de tarefas.")
                print(full_trace)
                return False

        self._human_delay("entre_tarefas")
        return True

    def run_consumer(self, ready_event: Event | None = None) -> None:
        consumer = KafkaTaskConsumer(
            topic=self.task_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.consumer_group,
        )

        print(f"🚀 Orquestrador online. Consumindo tarefas em {self.task_topic}")
        consumer.consume(self._handle_task, ready_event=ready_event)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consumidor de tarefas Instagram no Kafka")
    parser.add_argument("--task-topic", default="instagram.tasks")
    parser.add_argument("--user-state-topic", default="instagram.user.latest")
    parser.add_argument("--media-comments-state-topic", default="instagram.media.comments.latest")
    parser.add_argument("--user-stories-state-topic", default="instagram.user.stories.latest")
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    parser.add_argument("--group-id", default="instagram-orchestrator-workers")
    parser.add_argument("--delay-min", type=float, default=8.0)
    parser.add_argument("--delay-max", type=float, default=20.0)
    return parser.parse_args()


def run_orchestrator(cl: Client, args: argparse.Namespace) -> None:
    orchestrator = InstagramKafkaOrchestrator(
        client=cl,
        task_topic=args.task_topic,
        user_state_topic=args.user_state_topic,
        media_comments_state_topic=args.media_comments_state_topic,
        user_stories_state_topic=args.user_stories_state_topic,
        bootstrap_servers=args.bootstrap_server,
        consumer_group=args.group_id,
        min_delay_seconds=args.delay_min,
        max_delay_seconds=args.delay_max,
    )
    orchestrator.run_consumer()


if __name__ == "__main__":
    load_dotenv(find_dotenv())
    args = parse_args()
    client = login_with_persistence()
    run_orchestrator(cl=client, args=args)