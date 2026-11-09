from instagrapi import Client

class CommentService:
    def __init__(self, client: Client):
        self.cl = client

    @staticmethod
    def _canonical_media_id(media_id: str, user_id: str | None = None) -> str:
        value = str(media_id)
        if "_" in value or not user_id:
            return value
        return f"{value}_{user_id}"

    def fetch_media_comments(
        self,
        media_id: str,
        amount: int = 10,
        user_id: str | None = None,
    ):
        """Acesso direto à API para uso no orquestrador."""
        canonical_id = self._canonical_media_id(media_id, user_id)
        return self.cl.media_comments(canonical_id, amount=amount)

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
