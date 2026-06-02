from instagrapi import Client
from src.services.kafka.producer import KafkaService

class CommentService:
    def __init__(self, client: Client):
        # A sessão do Instagram agora é interna da classe
        self.cl = client
        self.kafka = KafkaService()

    def fetch_media_comments(self, media_id: str, amount: int = 10):
        """Acesso direto à API para uso no orquestrador."""
        return self.cl.media_comments(media_id, amount=amount)

    def get_comments(self, media_id: str, amount: int = 10):
        """Busca os comentários e retorna a lista bruta."""
        try:
            return self.cl.media_comments(media_id, amount=amount)
        except Exception as e:
            print(f"❌ Erro ao buscar comentários da mídia {media_id}: {e}")
            return []

    def download_by_media(self, media_id: str, amount: int = 10):
        """Tarefa: Baixar comentários de um post específico."""
        self.process_media_comments_task(media_id, amount=amount)

    def download_by_user(self, username: str, amount: int = 10):
        """Tarefa: método legado não suportado nesta versão."""
        raise NotImplementedError("Use MediaService para listar mídias e então download_by_media")