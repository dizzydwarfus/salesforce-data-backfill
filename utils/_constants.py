from dotenv import load_dotenv
import os


load_dotenv(".env")

# Salesforce Org Information
ARGUMENT_DICT = {
    "url": os.environ.get("PROD_DOMAIN"),
    "client_id": os.environ.get("PROD_CONSUMER_KEY"),
    "client_secret": os.environ.get("PROD_CONSUMER_SECRET"),
    "username": os.environ.get("PROD_USERNAME"),
    "password": os.environ.get("PROD_PASSWORD"),
    "security_token": os.environ.get("PROD_SECURITY_TOKEN"),
}

PAYLOAD = {
    "grant_type": "client_credentials",
    "client_id": ARGUMENT_DICT.get("client_id"),
    "client_secret": ARGUMENT_DICT.get("client_secret"),
}

DOMAIN = os.getenv("PROD_DOMAIN")
API_VERSION = os.getenv("API_VERSION")

# File Paths
SALES_MEMBERS_FILE = os.getenv("SALES_MEMBERS_FILE")
OUTPUT_FILE_LEAD_MEMBERS = os.getenv("OUTPUT_FILE_LEAD_MEMBERS")
OUTPUT_FILE_WON_OPPO_FC = os.getenv("OUTPUT_FILE_WON_OPPO_FC")
