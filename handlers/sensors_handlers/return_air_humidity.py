import os

from dotenv import load_dotenv
from handlers.common import store_data, get_data


load_dotenv(dotenv_path='.env')
TABLE_NAME = os.getenv('RETURN_AIR_HUMIDITY_TABLE_NAME')


def store_return_air_humidity(event, context):
    return store_data(event, context, TABLE_NAME, TABLE_NAME, "DOUBLE")

def get_return_air_humidity(event, context):
    return get_data(event, context, TABLE_NAME)