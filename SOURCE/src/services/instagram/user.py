from copy import deepcopy

from instagrapi import Client
from instagrapi.extractors import extract_user_v1
from pydantic import ValidationError

class UserService:
    def __init__(self, client: Client):
        self.cl = client

    def get_user_info_by_username(self, username: str):
        """Acesso direto à API para uso no orquestrador."""
        try:
            # Evita o fallback público automático quando somente o modelo de
            # broadcast diverge da resposta retornada pela API privada.
            return self.cl.user_info_by_username_v1(username)
        except ValidationError as exc:
            if not self._is_broadcast_members_validation(exc):
                raise

            last_json = getattr(self.cl, "last_json", None)
            raw_user = deepcopy(last_json.get("user")) if isinstance(last_json, dict) else None
            if not isinstance(raw_user, dict):
                raise

            channels = raw_user.get("pinned_channels_info", {}).get("pinned_channels_list", [])
            if not isinstance(channels, list):
                raise
            for channel in channels:
                if isinstance(channel, dict) and channel.get("number_of_members") is None:
                    channel["number_of_members"] = 0

            print(
                "Resposta de broadcast compatibilizada: "
                "number_of_members ausente foi normalizado para 0"
            )
            return extract_user_v1(raw_user)

    @staticmethod
    def _is_broadcast_members_validation(exc: ValidationError) -> bool:
        return any(
            error.get("type") == "int_type"
            and error.get("input") is None
            and "number_of_members" in error.get("loc", ())
            for error in exc.errors()
        )

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
