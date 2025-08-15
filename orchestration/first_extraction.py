import sys
import os
from google.cloud import storage

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from src.extraction.common.bling_api_client import BlingClient
from src.extraction import sales, sales_channels, products, product_categories, sales_status
from src.extraction.common.state_manager import FileStateManager

def extraction():
    state_manager = FileStateManager()
    cloud_storage_client = storage.Client(project="eletrofor")
    bucket = cloud_storage_client.bucket("bling-raw-data")

    client = BlingClient(state_manager=state_manager)

    product_categories.extract_product_categories(client=client, storage_bucket=bucket)
    sales_channels.extract_sales_channels(client=client, storage_bucket=bucket)
    sales_status.extract_sales_status(client=client, storage_bucket=bucket)
    products.products_extraction(client=client, storage_bucket=bucket)
    sales.sales_extraction(client=client, dataInicial="2025-01-01", dataFinal="2025-08-09", storage_bucket=bucket)

extraction()