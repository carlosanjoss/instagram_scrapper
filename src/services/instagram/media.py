from instagrapi import Client
from src.services.kafka.producer import KafkaService
from src.utils.formatters import convert_model_to_json

class MediaService:
    def __init__(self, client: Client):
        self.cl = client
        self.kafka = KafkaService()

    def get_user_medias(self, user_id: str, amount: int = 10):
        """Acesso direto à API para buscar mídias por user_id."""
        return self.cl.user_medias(user_id, amount=amount)

    def get_media_info(self, media_id: str):
        """Acesso direto à API para buscar detalhes de uma mídia."""
        return self.cl.media_info(media_id)

    def list_medias_users(self, username: str, amount: int = 10):
        """Listar os posts recentes de uma conta específica."""
        try:
            user_id = self.cl.user_id_from_username(username)
            return self.cl.user_medias(user_id, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def download_medias_users(self, username: str, amount: int = 10):
        """Download dos posts recentes de uma conta específica."""
        try:
            for media in self.list_medias_users(username, amount=amount):
                if media.media_type == 1:
                    self.cl.photo_download(media.pk) # adicionar local de armazenamento
                elif media.media_type == 2:
                    self.cl.video_download(media.pk) # adicionar local de armazenamento
                elif media.media_type == 8:
                    self.cl.album_download(media.pk) # adicionar local de armazenamento
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def like_medias_users(self, username: str, amount: int = 10):
        """Curtir os posts recentes de uma conta específica."""
        try:
            user_id = self.cl.user_id_from_username(username)
            for media in self.cl.user_medias(user_id, amount=amount):
                print(self.cl.media_likers(media.id)) # adicionar local de armazenamento
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
    
    def list_medias_users_marked(self, username: str, amount: int = 10):
        """Listar os posts recentes em que a conta específica foi marcada."""
        try:
            user_id = self.cl.user_id_from_username(username)
            return self.cl.medias_marked(user_id, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
        
    def download_medias_users_marked(self, username: str, amount: int = 10):
        """Download dos posts recentes em que a conta específica foi marcada."""
        try:
            for media in self.list_medias_users_marked(username, amount=amount):
                if media.media_type == 1:
                    self.cl.photo_download(media.pk) # adicionar local de armazenamento
                elif media.media_type == 2:
                    self.cl.video_download(media.pk) # adicionar local de armazenamento
                elif media.media_type == 8:
                    self.cl.album_download(media.pk) # adicionar local de armazenamento
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
        
    def list_reels_users(self, username: str, amount: int = 10):
        """Listar os reels recentes de uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
                return self.cl.user_reels(user_id, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
        
    def download_reels_users(self, username: str, amount: int = 10):
        """Download dos reels recentes de uma conta específica."""
        try:
            for media in self.list_reels_users(username, amount=amount):
                if media.media_type == 2:
                    self.cl.video_download(media.pk) # adicionar local de armazenamento
                else:
                    print(f"Reel {media.pk} não é um vídeo, pulando download.")
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
        
    def info_media(self, media_id: str):
        """Informações de um post específico."""
        try:
            if self.cl is not None:
                return self.cl.media_info(media_id)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return