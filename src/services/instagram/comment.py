from instagrapi import Client

class CommentService:
    def __init__(self, client: Client):
        self.cl = client

    def fetch_media_comments(self, media_id: str, amount: int = 10):
        """Acesso direto à API para uso no orquestrador."""
        return self.cl.media_comments(media_id, amount=amount)

    def get_comments(self, media_id: str, amount: int = 10):
        """Busca os comentários e retorna a lista bruta."""
        try:
            return self.cl.media_comments(media_id, amount=amount)
        except Exception as e:
            print(f"Erro ao buscar comentários da mídia {media_id}: {e}")
            return []

    def download_by_media(self, media_id: str, amount: int = 10):
        """Compatibilidade: busca e retorna comentários de uma mídia."""
        return self.get_comments(media_id, amount=amount)

    def download_by_user(self, username: str, amount: int = 10):
        """Tarefa: método legado não suportado nesta versão."""
        raise NotImplementedError("Use MediaService para listar mídias e então download_by_media")
