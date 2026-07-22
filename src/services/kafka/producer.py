import json
from typing import Any, Dict, Iterable, Tuple

from kafka import KafkaConsumer, KafkaProducer, TopicPartition


KafkaRecord = Tuple[str, str | None, Dict[str, Any]]


class KafkaService:
    """Produz registros comuns e commits Kafka atômicos."""

    def __init__(self, bootstrap_servers: str = "localhost:29092", transactional_id: str | None = None):
        self.bootstrap_servers = bootstrap_servers
        options = {
            "bootstrap_servers": [bootstrap_servers],
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
            bootstrap_servers=[self.bootstrap_servers],
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
