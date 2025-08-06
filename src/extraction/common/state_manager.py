import json
import os
from threading import Lock
from typing import Optional
from datetime import datetime, timezone

class FileStateManager:
    def __init__(self, state_path: str = "state.json"):
        self.state_path = state_path
        self.lock = Lock()
        self._state = self._load_state()

    def _load_state(self) -> dict:
        with self.lock:
            if not os.path.exists(self.state_path):
                return {}
            try:
                with open(self.state_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}

    def get_state(self, key: str) -> Optional[str]:
        return self._state.get(key)

    def set_state(self, key: str, value: str):
        with self.lock:
            self._state[key] = value
            self._state['last_updated_at'] = datetime.now(timezone.utc).isoformat()
            try:
                with open(self.state_path, 'w', encoding='utf-8') as f:
                    json.dump(self._state, f, indent=4)
            except IOError as e:
                print(f"ERRO CRÍTICO: Não foi possível salvar o arquivo de estado em '{self.state_path}': {e}")