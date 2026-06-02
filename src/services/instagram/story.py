from instagrapi import Client
from src.services.kafka.producer import KafkaService
from src.utils.formatters import convert_model_to_json

class StoryService:
    def __init__(self, client: Client):
        self.cl = client
        self.kafka = KafkaService()

    def get_user_stories(self, user_id: str, amount: int = 10):
        """Acesso direto à API para buscar stories por user_id."""
        return self.cl.user_stories(user_id, amount=amount)

    def list_storys_users(self, username: str, amount: int = 10):
        """Listar os stories recentes de uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
                return self.cl.user_stories(user_id, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def download_storys_users(self, username: str, amount: int = 10):
        """Download de stories recentes de uma conta específica."""
        try:
            for media in self.list_storys_users(username, amount=amount):
                self.cl.story_download(media.pk) # adicionar local de armazenamento
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def like_storys_users(self, username: str, amount: int = 10):
        """Like de stories recentes de uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
                for media in self.cl.user_stories(user_id, amount=amount):
                    print(self.cl.story_like(media.id, revert=False))
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
        
    def info_story(self, story_id: str):
        """Informações de um story específico."""
        try:
            if self.cl is not None:
                return self.cl.story_info(story_id)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return