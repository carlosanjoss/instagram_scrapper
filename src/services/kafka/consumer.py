import json
from typing import Callable

from kafka import KafkaConsumer


class KafkaTaskConsumer:
    """Consumidor coordenado por grupo; o handler confirma offsets via transação."""

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str) -> None:
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id

    def consume(self, handler: Callable[[object, object], bool], ready_event=None) -> None:
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.bootstrap_servers],
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            isolation_level="read_committed",
            max_poll_records=1,
            max_poll_interval_ms=1_800_000,
        )
        if ready_event is not None:
            ready_event.set()
        print(f"Consumidor Kafka online. Tópico: {self.topic}")
        try:
            while True:
                records = consumer.poll(timeout_ms=1000, max_records=1)
                for batch in records.values():
                    for message in batch:
                        if not handler(message, consumer):
                            return
        finally:
            consumer.close()
