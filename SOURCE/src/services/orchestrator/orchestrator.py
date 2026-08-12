import random
import time
import traceback
from datetime import datetime, timezone
from threading import Event
from typing import Any, Dict

from instagrapi import Client
from kafka import OffsetAndMetadata, TopicPartition

from src.services.instagram.comment import CommentService
from src.services.instagram.media import MediaService
from src.services.instagram.story import StoryService
from src.services.instagram.user import UserService
from src.services.kafka.consumer import KafkaTaskConsumer
from src.services.kafka.producer import KafkaService
from src.services.orchestrator.handlers import (
    FetchCommentsHandler,
    FetchMediaHandler,
    FetchStoryHandler,
    FetchUserHandler,
)
from src.services.orchestrator.retry_policy import RetryPolicy
from src.services.orchestrator.snapshot_repository import SnapshotRepository
from src.services.orchestrator.task_contract import ExecutionResult, KafkaRecord, Task
from src.services.orchestrator.task_executor import TaskExecutor


class InstagramKafkaOrchestrator:
    """Coordena consumo, idempotência e commit transacional das tarefas."""

    def __init__(
        self,
        client: Client,
        task_topic: str = "instagram.tasks",
        user_state_topic: str = "instagram.user.latest",
        media_comments_state_topic: str = "instagram.media.comments.latest",
        user_stories_state_topic: str = "instagram.user.stories.latest",
        processed_task_topic: str = "instagram.tasks.processed",
        user_observation_topic: str = "instagram.user.observations",
        media_observation_topic: str = "instagram.media.observations",
        bootstrap_servers: str = "localhost:29092",
        consumer_group: str = "instagram-orchestrator-workers",
        min_delay_seconds: float = 8.0,
        max_delay_seconds: float = 40.0,
    ) -> None:
        self.task_topic = task_topic
        self.processed_task_topic = processed_task_topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.kafka = KafkaService(
            bootstrap_servers=bootstrap_servers,
            transactional_id=f"{consumer_group}-transactions",
        )
        self.processed_tasks = self.kafka.get_all_latest_by_key(processed_task_topic)
        self.snapshots = SnapshotRepository(
            kafka=self.kafka,
            user_topic=user_state_topic,
            media_comments_topic=media_comments_state_topic,
            user_stories_topic=user_stories_state_topic,
        )

        user_service = UserService(client)
        media_service = MediaService(client)
        handlers = {
            "fetch_user": FetchUserHandler(
                user_service=user_service,
                media_service=media_service,
                snapshots=self.snapshots,
                task_topic=task_topic,
                user_observation_topic=user_observation_topic,
                delay=self._human_delay,
            ),
            "fetch_media": FetchMediaHandler(
                media_service=media_service,
                snapshots=self.snapshots,
                task_topic=task_topic,
                media_observation_topic=media_observation_topic,
                delay=self._human_delay,
            ),
            "fetch_story": FetchStoryHandler(
                story_service=StoryService(client),
                snapshots=self.snapshots,
                delay=self._human_delay,
            ),
            "fetch_comments": FetchCommentsHandler(
                comment_service=CommentService(client),
                delay=self._human_delay,
            ),
        }
        self.executor = TaskExecutor(handlers=handlers, retry_policy=RetryPolicy(attempts=3))

    def _human_delay(self, context: str) -> None:
        wait = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        print(f"Pausa ({context}) por {wait:.2f}s para reduzir padrão automatizado")
        time.sleep(wait)

    def _terminal_record(
        self,
        task_id: str,
        task: Task,
        status: str,
        error: str | None = None,
    ) -> tuple[KafkaRecord, Dict[str, Any]]:
        payload = {
            "task_id": task_id,
            "root_task_id": task.get("root_task_id"),
            "type": task.get("type"),
            "status": status,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        if error:
            payload["error"] = error
        return (self.processed_task_topic, task_id, payload), payload

    def _commit(self, message, records: list[KafkaRecord]) -> None:
        partition = TopicPartition(message.topic, message.partition)
        leader_epoch = getattr(message, "leader_epoch", -1)
        if leader_epoch is None:
            leader_epoch = -1
        offsets = {partition: OffsetAndMetadata(message.offset + 1, "", leader_epoch)}
        self.kafka.commit_records_and_offset(records, offsets, self.consumer_group)

    @staticmethod
    def _error_text(result: ExecutionResult) -> str:
        error = result.error or RuntimeError("Falha sem erro informado")
        return f"{type(error).__name__}: {error}"

    def _handle_task(self, message, consumer) -> bool:
        task = message.value if isinstance(message.value, dict) else {}
        task_id = str(task.get("task_id") or f"invalid:{message.topic}:{message.partition}:{message.offset}")

        if task_id in self.processed_tasks:
            self._commit(message, [])
            return True

        result = self.executor.execute(task)
        if result.status == "fatal":
            print("Sessão do Instagram inválida. Offset não confirmado.")
            if result.error is not None:
                print("".join(traceback.format_exception(type(result.error), result.error, result.error.__traceback__)))
            return False

        error_text = self._error_text(result) if result.status == "failed" else None
        terminal, terminal_payload = self._terminal_record(task_id, task, result.status, error_text)
        records = [*result.handler_result.records, terminal]
        self._commit(message, records)
        self.snapshots.commit(result.handler_result.snapshot_updates)
        self.processed_tasks[task_id] = terminal_payload

        if result.status == "failed":
            print(f"Tarefa {task_id} descartada após falha: {error_text}")
        else:
            self._human_delay("entre_tarefas")
        return True

    def run_consumer(
        self,
        ready_event: Event | None = None,
        stopped_event: Event | None = None,
    ) -> None:
        try:
            consumer = KafkaTaskConsumer(
                topic=self.task_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
            )
            print(f"Orquestrador online. Consumindo tarefas em {self.task_topic}")
            consumer.consume(self._handle_task, ready_event=ready_event)
        finally:
            if stopped_event is not None:
                stopped_event.set()
