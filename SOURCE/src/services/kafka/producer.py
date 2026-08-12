import json
from typing import Any, Dict, Iterable, Tuple

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from src.services.kafka.config import kafka_connection_options


KafkaRecord = Tuple[str, str | None, Dict[str, Any]]


class KafkaService:
    """Produz registros comuns e commits Kafka atômicos."""

    DEFAULT_TOPICS = (
        {"name": "instagram.tasks"},
        {"name": "instagram.user.data"},
        {"name": "instagram.media.data"},
        {"name": "instagram.user.observations", "retention_ms": 15552000000},
        {"name": "instagram.media.observations", "retention_ms": 15552000000},
        {"name": "instagram.comments.data"},
        {"name": "instagram.stories.data"},
        {"name": "instagram.tasks.processed", "cleanup_policy": "compact"},
        {"name": "instagram.user.latest", "cleanup_policy": "compact"},
        {"name": "instagram.media.comments.latest", "cleanup_policy": "compact"},
        {"name": "instagram.user.stories.latest", "cleanup_policy": "compact"},
    )

    def __init__(self, bootstrap_servers: str = "localhost:29092", transactional_id: str | None = None):
        self.bootstrap_servers = bootstrap_servers
        self.connection_options = kafka_connection_options(bootstrap_servers)
        self._ensure_topics()
        options = {
            **self.connection_options,
            "acks": "all",
            "retries": 5,
        }
        if transactional_id:
            options["transactional_id"] = transactional_id
            options["enable_idempotence"] = True
        self.producer = KafkaProducer(**options)
        self.transactional = transactional_id is not None
        if self.transactional:
            self.producer.init_transactions()

    def _ensure_topics(self) -> None:
        admin = KafkaAdminClient(**self.connection_options, client_id="instagram-kafka-admin")
        try:
            existing = set(admin.list_topics())
            topics = []
            for topic in self.DEFAULT_TOPICS:
                name = topic["name"]
                if name in existing:
                    continue
                configs = {}
                if topic.get("retention_ms") is not None:
                    configs["retention.ms"] = str(topic["retention_ms"])
                if topic.get("cleanup_policy") is not None:
                    configs["cleanup.policy"] = topic["cleanup_policy"]
                topics.append(
                    NewTopic(
                        name=name,
                        num_partitions=1,
                        replication_factor=1,
                        topic_configs=configs or None,
                    )
                )
            if topics:
                admin.create_topics(new_topics=topics, validate_only=False)
        finally:
            admin.close()

    @staticmethod
    def _serialize(data: Any) -> bytes:
        if isinstance(data, str):
            return data.encode("utf-8")
        return json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")

    def _send(self, topic: str, data: Any, key: str | None = None):
        key_bytes = str(key).encode("utf-8") if key is not None else None
        return self.producer.send(topic, key=key_bytes, value=self._serialize(data))

    def send_to_topic(self, topic: str, data: Any, key: str | None = None) -> None:
        self._send(topic, data, key).get(timeout=30)

    def commit_records_and_offset(
        self,
        records: Iterable[KafkaRecord],
        offsets: Dict[TopicPartition, Any],
        group_id: str,
    ) -> None:
        if not self.transactional:
            raise RuntimeError("Produtor não foi configurado para transações")
        self.producer.begin_transaction()
        try:
            for topic, key, data in records:
                self._send(topic, data, key)
            self.producer.send_offsets_to_transaction(offsets, group_id)
            self.producer.commit_transaction()
        except Exception:
            self.producer.abort_transaction()
            raise

    def get_all_latest_by_key(self, topic: str) -> Dict[str, Dict[str, Any]]:
        consumer = KafkaConsumer(
            **self.connection_options,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m is not None else None,
            key_deserializer=lambda m: m.decode("utf-8") if m else None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
            isolation_level="read_committed",
        )
        latest: Dict[str, Dict[str, Any]] = {}
        try:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return latest
            topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
            consumer.assign(topic_partitions)
            consumer.seek_to_beginning(*topic_partitions)
            for message in consumer:
                if message.key is not None:
                    if message.value is None:
                        latest.pop(message.key, None)
                    else:
                        latest[message.key] = message.value
            return latest
        finally:
            consumer.close()

    def get_latest_by_key(self, topic: str, key: str):
        return self.get_all_latest_by_key(topic).get(str(key))
