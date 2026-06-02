from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json

class KafkaService:
    def __init__(self, bootstrap_servers: str = 'localhost:29092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers]
        )

    def send_to_topic(self, topic, data, key=None):
        # Aqui você centraliza logs e tratamentos de erro
        print(f"Enviando dados para {topic}...")
        value_bytes = data.encode('utf-8') if isinstance(data, str) else json.dumps(
            data,
            ensure_ascii=False,
            default=str,
        ).encode('utf-8')
        key_bytes = str(key).encode('utf-8') if key is not None else None
        self.producer.send(topic, key=key_bytes, value=value_bytes)
        self.producer.flush()

    def get_latest_by_key(self, topic: str, key: str):
        """Retorna a última mensagem do tópico para uma key específica.

        Faz varredura do tópico desde o início e mantém o último registro da key.
        Útil para tópicos de estado (snapshot) com cardinalidade controlada.
        """
        consumer = KafkaConsumer(
            bootstrap_servers=[self.bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m is not None else None,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
        )

        try:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return None

            topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
            consumer.assign(topic_partitions)
            for partition in topic_partitions:
                consumer.seek_to_beginning(partition)

            latest_value = None
            target_key = str(key)
            for message in consumer:
                if message.key == target_key:
                    latest_value = message.value

            return latest_value
        finally:
            consumer.close()