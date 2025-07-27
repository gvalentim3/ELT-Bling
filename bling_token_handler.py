import requests
import base64
import os
from dotenv import load_dotenv, set_key

load_dotenv()

DOTENV_PATH = ".env"

class BlingTokenHandler():
    def __init__(self):
        self.url = "https://api.bling.com.br/Api/v3/oauth/token"
        self.client_id = os.getenv("BLING_CLIENT_ID")
        self.client_secret = os.getenv("BLING_CLIENT_SECRET")
        self.authorization_code = os.getenv("BLING_AUTH_CODE")

        self.credentials = f"{self.client_id}:{self.client_secret}"
        self.credentials_base64 = base64.b64encode(self.credentials.encode('utf-8')).decode('utf-8')

    def get_tokens(self):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "1.0",
            "Authorization": f"Basic {self.credentials_base64}"
        }

        data = {
            "grant_type": "authorization_code",
            "code": self.authorization_code
        }

        response = requests.post(self.url, headers=headers, data=data)

        if response.status_code == 200:
            print("Tokens obtido com sucesso!")
            payload = response.json()
            set_key(DOTENV_PATH, "BLING_ACCESS_TOKEN", payload["access_token"])
            set_key(DOTENV_PATH, "BLING_REFRESH_TOKEN", payload["refresh_token"])
            
        else:
            print("Erro na requisição:", response.status_code)
            print(response.text)

    def update_access_token(self):
        refresh_token = os.getenv("BLING_REFRESH_TOKEN")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "1.0",
            "Authorization": f"Basic {self.credentials_base64}"
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }

        response = requests.post(self.url, headers=headers, data=data)

        if response.status_code == 200:
            print("Token renovado com sucesso!")
            payload = response.json()
            set_key(DOTENV_PATH, "BLING_ACCESS_TOKEN", payload["access_token"], quote_mode="never")
            set_key(DOTENV_PATH, "BLING_REFRESH_TOKEN", payload["refresh_token"], quote_mode="never")

            
        else:
            print("Erro na requisição:", response.status_code)
            print("Detalhes do erro:", response.text)

token_handler = BlingTokenHandler()

token_handler.update_access_token()