import os

from dotenv import load_dotenv
from handlers.common import store_data, get_data


load_dotenv(dotenv_path='.env')
TABLE_NAME = os.getenv('VIBRATION_SENSOR_TABLE_NAME')


def store_vibration_sensor(event, context):
    return store_data(event, context, TABLE_NAME, TABLE_NAME, "DOUBLE")

def get_vibration_sensor(event, context):
    return get_data(event, context, TABLE_NAME)