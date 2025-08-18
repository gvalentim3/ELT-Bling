from typing import Dict, Optional
import requests
import base64

from .secret_manager import SecretManagerStateManager

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

    def __init__(self, state_manager: SecretManagerStateManager):
        self.state_manager = state_manager
        
        self._access_token = None
        self._refresh_token = self.state_manager.get_state("ELETROFOR_BLING_REFRESH_TOKEN")

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
        self.state_manager.set_state("ELETROFOR_BLING_REFRESH_TOKEN", self._refresh_token)
        
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