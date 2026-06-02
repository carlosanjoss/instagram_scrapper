from instagrapi import Client
from src.services.kafka.producer import KafkaService

class HashtagService:
    def __init__(self, client: Client):
        self.cl = client
        self.kafka = KafkaService()

    def info_hashtag(self, hashtag: str):
        """Informações de uma hashtag específica."""
        try:
            if self.cl is not None:
                return self.cl.hashtag_info(hashtag)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def list_hastags_relateds(self, hashtag: str):
        """Listar hashtags relacionadas a uma hashtag específica."""
        try:
            if self.cl is not None:
                return self.cl.hashtag_related_hashtags(hashtag)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def list_top_medias_hashtag(self, hashtag: str, amount: int = 10):
        """Listar os posts mais populares de uma hashtag específica."""
        try:
            if self.cl is not None:
                return self.cl.hashtag_medias_top(hashtag, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
        
    def list_recent_medias_hashtag(self, hashtag: str, amount: int = 10):
        """Listar os posts mais recentes de uma hashtag específica."""
        try:
            if self.cl is not None:
                return self.cl.hashtag_medias_recent(hashtag, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return