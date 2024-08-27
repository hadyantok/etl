from dotenv import load_dotenv
import os
import pathlib

# Determine the path to the .env file
env_path = pathlib.Path(__file__).resolve().parent.parent.parent / '.env'

# Load environment variables from the specified .env file
load_dotenv(dotenv_path=env_path)

CANDY_DATABASE_URL = os.getenv('CANDY_DATABASE_URL')
SERVICE_ACCOUNT_PATH = os.getenv('SERVICE_ACCOUNT_PATH')