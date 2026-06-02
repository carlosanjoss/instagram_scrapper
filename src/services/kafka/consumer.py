import json
import time
from typing import Any, Callable, Dict

from kafka import KafkaConsumer, TopicPartition


class KafkaTaskConsumer:
	"""Encapsula o loop de consumo de tarefas Kafka."""

	def __init__(
		self,
		topic: str,
		bootstrap_servers: str = "localhost:29092",
		group_id: str = "instagram-orchestrator-workers",
		auto_offset_reset: str = "earliest",
		enable_auto_commit: bool = True,
	) -> None:
		self.topic = topic
		self.bootstrap_servers = bootstrap_servers
		self.group_id = group_id
		self.auto_offset_reset = auto_offset_reset
		self.enable_auto_commit = enable_auto_commit

	def consume(
		self,
		handler: Callable[[Dict[str, Any]], bool],
		ready_event=None,
	) -> None:
		consumer = KafkaConsumer(
			bootstrap_servers=[self.bootstrap_servers],
			value_deserializer=lambda m: json.loads(m.decode("utf-8")),
			auto_offset_reset=self.auto_offset_reset,
			enable_auto_commit=False,
		)

		partitions = None
		while not partitions:
			partitions = consumer.partitions_for_topic(self.topic)
			if not partitions:
				print(f"⏳ Aguardando metadata do tópico: {self.topic}")
				time.sleep(1)

		topic_partitions = [TopicPartition(self.topic, partition) for partition in sorted(partitions)]
		consumer.assign(topic_partitions)
		consumer.seek_to_end(*topic_partitions)

		if ready_event is not None:
			ready_event.set()

		print(f"🚀 Consumidor Kafka online. Tópico: {self.topic}")
		while True:
			records = consumer.poll(timeout_ms=1000, max_records=50)
			if not records:
				continue

			for batch in records.values():
				for message in batch:
					task = message.value
					should_continue = handler(task)
					if not should_continue:
						print("🛑 Consumo Kafka interrompido por erro crítico de autenticação Instagram.")
						consumer.close()
						return
