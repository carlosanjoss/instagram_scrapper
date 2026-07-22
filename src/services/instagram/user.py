from instagrapi import Client

class UserService:
    def __init__(self, client: Client):
        self.cl = client

    def get_user_info_by_username(self, username: str):
        """Acesso direto à API para uso no orquestrador."""
        return self.cl.user_info_by_username(username)

    def get_user_id_from_username(self, username: str):
        """Resolve user_id diretamente pela API."""
        return self.cl.user_id_from_username(username)

    def info_user(self, username: str):
        """Informações de uma conta específica."""
        try:
            if self.cl is not None:
                return self.cl.user_info_by_username(username)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
    
    def info_user_id(self, userid: str):
        """ID de uma conta específica."""
        try:
            if self.cl is not None:
                return self.cl.user_info(userid)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
    
    def list_followers_users(self, username: str, amount: int = 10):
        """Listar os seguidores de uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
                return self.cl.user_followers(user_id, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return

    def list_following_users(self, username: str, amount: int = 10):
        """Listar as contas seguidas por uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
                return self.cl.user_following(user_id, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
    
    def search_followers_users(self, username: str, query: str, amount: int = 10):
        """Pesquisar seguidores de uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
                return self.cl.user_followers(user_id, query=query, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
    
    def search_following_users(self, username: str, query: str, amount: int = 10):
        """Pesquisar contas seguidas por uma conta específica."""
        try:
            if self.cl is not None:
                user_id = self.cl.user_id_from_username(username)
                return self.cl.user_following(user_id, query=query, amount=amount)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
        
    def id_from_username(self, username: str):
        """ID de uma conta específica a partir do nome de usuário."""
        try:
            if self.cl is not None:
                return self.cl.user_id_from_username(username)
        except Exception as e:
            print(f"Erro ao fazer login: {e}")
            return
