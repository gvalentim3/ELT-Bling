from typing import Dict, Optional
import requests
import base64
import os
from dotenv import load_dotenv, set_key

class BlingClient():
    def __init__(self):
        self.DOTENV_PATH = ".env"
        load_dotenv(self.DOTENV_PATH)

        self.BASE_URL = "https://api.bling.com.br/Api/v3"

    def _get_env_var(self, var_name: str, required: bool = True) -> Optional[str]:
        value = os.getenv(var_name)
        if required and value is None:
            raise ValueError(f"Variável de ambiente {var_name} não encontrada!")
        return value

    def _get_access_token(self) -> str:
        load_dotenv(self.DOTENV_PATH, override=True)
        token = self._get_env_var("BLING_ACCESS_TOKEN")
        if not token:
            raise ValueError("Access token não disponível. Execute generate_tokens() primeiro.")
        return token

    def _get_auth_headers(self) -> Dict[str, str]:
        client_id = self._get_env_var("BLING_CLIENT_ID")
        client_secret = self._get_env_var("BLING_CLIENT_SECRET")

        credentials = f"{client_id}:{client_secret}"
        credentials_base64 = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "1.0",
            "Authorization": f"Basic {credentials_base64}"
        }
    
    def get_default_headers(self) -> Dict[str, str]:
        access_token = self._get_access_token()

        return {
            "accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
    
    def get_api_url(self, endpoint: str) -> str:
        return f"{self.BASE_URL}/{endpoint}"
    
    def ensure_valid_token(self):
        try:
            self._get_access_token()
        except ValueError:
            self.generate_tokens()
        
        return self.refresh_access_token()

    def generate_tokens(self) -> None:
        url = f"{self.BASE_URL}/oauth/token"

        response = requests.post(
            url=url,
            headers=self._get_auth_headers(),
            data={
                "grant_type": "authorization_code",
                "code": self._get_env_var("BLING_AUTH_CODE")
            }
        )

        self._handle_token_response(response)

    def refresh_access_token(self) -> None:
        refresh_token = self._get_env_var("BLING_REFRESH_TOKEN")

        response = requests.post(
            url=f"{self.BASE_URL}/oauth/token",
            headers=self._get_auth_headers(),
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }
        )

        self._handle_token_response(response)

    def _handle_token_response(self, response: requests.Response) -> None:
        if response.status_code == 200:
            payload = response.json()
            set_key(self.DOTENV_PATH, "BLING_ACCESS_TOKEN", payload["access_token"], quote_mode="never")
            set_key(self.DOTENV_PATH, "BLING_REFRESH_TOKEN", payload["refresh_token"], quote_mode="never")
            load_dotenv(self.DOTENV_PATH, override=True)
            print("Tokens atualizados com sucesso!")
        else:
            error_msg = f"Erro {response.status_code}: {response.text}"
            raise requests.exceptions.HTTPError(error_msg)
        

# Em src/common/bling_client.py
import base64
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict

from . import config

logger = logging.getLogger(__name__)

class BlingClient:
    BASE_URL = "https://api.bling.com.br/Api/v3"

    def __init__(self, state_manager):
        self.state_manager = state_manager
        
        self._access_token = None
        self._refresh_token = self.state_manager.get_state("BLING_REFRESH_TOKEN") or config.BLING_REFRESH_TOKEN

        self.session = self._create_resilient_session()
        self.authenticate()

    def _create_resilient_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    def _get_auth_headers(self) -> Dict[str, str]:
        credentials = f"{config.BLING_CLIENT_ID}:{config.BLING_CLIENT_SECRET}"
        b64_creds = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "1.0",
            "Authorization": f"Basic {b64_creds}"
            }

    def authenticate(self):
        if not self._refresh_token:
            raise ValueError("Refresh Token não encontrado. Gere um novo com o Auth Code.")
        
        try:
            self._perform_token_refresh()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Falha ao renovar token. Verifique seu REFRESH_TOKEN. Erro: {e}")
            raise

    def _perform_token_refresh(self):
        url = f"{self.BASE_URL}/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        }
        headers = self._get_auth_headers()
        
        response = self.session.post(url, data=data, headers=headers)
        response.raise_for_status()
        
        payload = response.json()
        self._access_token = payload["access_token"]
        
        self._refresh_token = payload["refresh_token"]
        self.state_manager.set_state("BLING_REFRESH_TOKEN", self._refresh_token)
        
        logger.info("Access Token do Bling renovado com sucesso!")

    def get(self, endpoint: str, params: Dict = None) -> requests.Response:
        if not self._access_token:
            self.authenticate()

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"Authorization": f"Bearer {self._access_token}"}
        
        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning("Access Token expirado. Tentando renovar.")
                self.authenticate()

                headers["Authorization"] = f"Bearer {self._access_token}"
                response = self.session.get(url, headers=headers, params=params)
                response.raise_for_status()
                return response
            else:
                raise