from datetime import datetime
import requests
import json
from pathlib import Path
from .services.bling_api_client import BlingClient
from typing import Dict, Any

def extract_sales_situations(client: BlingClient) -> Dict[str, Any]:
    url = client.get_api_url("situacoes/modulos/98310")
    headers = client.get_default_headers()

    response = requests.get(
        url=url,
        headers=headers,
        timeout=10
    )
    response.raise_for_status()

    metadata = {
        "extraction_date": datetime.now()
    }

    return {
        "metadata": metadata,
        "data": response.json()
    }

def save_raw_sales_situations(data: Dict[str, Any], output_dir: Path) -> None:
    output_file = output_dir / "sales_situations.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)  

def main(client: BlingClient) -> None:
    try:
        raw_data = extract_sales_situations(client=client)
        output_dir = Path(__file__).parent.parent / "data/raw"
        save_raw_sales_situations(raw_data, output_dir)
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise