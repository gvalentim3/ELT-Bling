import json
from typing import Optional, Dict
from datetime import datetime, timezone
from google.cloud import secretmanager
from google.api_core import exceptions

class SecretManagerStateManager:
    def __init__(self, project_id: str, secret_id: str):
        if not project_id or not secret_id:
            raise ValueError("project_id e secret_id são obrigatórios.")
            
        self.project_id = project_id
        self.secret_id = secret_id
        self.client = secretmanager.SecretManagerServiceClient()
        self._state: Dict = self._load_state()

    def _load_state(self) -> Dict:
        secret_name = f"projects/{self.project_id}/secrets/{self.secret_id}/versions/latest"
        
        try:
            response = self.client.access_secret_version(name=secret_name)
            payload = response.payload.data.decode("UTF-8")
            print(payload)
            return json.loads(payload)
        except exceptions.NotFound:
            print(f"Aviso: Segredo '{self.secret_id}' não encontrado. Retornando estado vazio.")
            return {}
        except (json.JSONDecodeError, AttributeError):
            print(f"Aviso: O conteúdo do segredo '{self.secret_id}' não é um JSON válido. Retornando estado vazio.")
            return {}

    def get_state(self, key: str) -> Optional[str]:
        return self._state.get(key)

    def set_state(self, key: str, value: str):
        self._state[key] = value
        self._state['last_updated_at'] = datetime.now(timezone.utc).isoformat()
        
        secret_parent = f"projects/{self.project_id}/secrets/{self.secret_id}"
        new_payload = json.dumps(self._state, indent=4).encode("UTF-8")
        
        self.client.add_secret_version(
            parent=secret_parent, payload={"data": new_payload}
        )