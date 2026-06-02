from instagrapi import Client
from src.services.kafka.producer import KafkaService
from src.utils.formatters import convert_model_to_json

class HighlightService:

    def __init__(self, client: Client):
        self.cl = client
        self.kafka = KafkaService()

    def process_highlight_task(self, user_id: str, highlight_id: str):
        """Tratamento e envio para o Kafka."""
        try:
            highlight = self.cl.highlight_info(highlight_id)
            self.kafka.send_to_topic(
                topic="instagram.highlights.data",
                key=user_id,
                data=convert_model_to_json(highlight)
            )
        except Exception as e:
            print(f"❌ Erro ao buscar destaque {highlight_id}: {e}")
            return

    def info_highlight(self, highlight_id: str):
        """Informações de um destaque específico."""
        try:
            if self.cl is not None:
                return self.cl.highlight_info(highlight_id)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def highlight_users(self, username: str):
        """Listar os destaques de uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
            return self.cl.user_highlights(user_id)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return