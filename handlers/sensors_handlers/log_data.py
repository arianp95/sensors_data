import os

from dotenv import load_dotenv
from handlers.common import store_data, get_data


load_dotenv(dotenv_path='.env')
TABLE_NAME = os.getenv('LOG_DATA_TABLE_NAME')


def get_log_data(event, context):
    return get_data(event, context, TABLE_NAME)