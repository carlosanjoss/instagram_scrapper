from instagrapi import Client

class HighlightService:

    def __init__(self, client: Client):
        self.cl = client

    def get_highlight(self, highlight_id: str):
        """Busca um destaque e propaga falhas para a camada chamadora."""
        return self.cl.highlight_info(highlight_id)

    def process_highlight_task(self, user_id: str, highlight_id: str):
        """Compatibilidade: busca um destaque sem publicá-lo diretamente."""
        try:
            return self.get_highlight(highlight_id)
        except Exception as e:
            print(f"Erro ao buscar destaque {highlight_id}: {e}")
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
