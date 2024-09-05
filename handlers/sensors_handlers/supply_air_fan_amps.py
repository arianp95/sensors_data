import os

from dotenv import load_dotenv
from handlers.common import store_data, get_data


load_dotenv(dotenv_path='.env')
TABLE_NAME = os.getenv('SUPPLY_AIR_FAN_AMPS_TABLE_NAME')


def store_supply_air_fan_amps(event, context):
    return store_data(event, context, TABLE_NAME, TABLE_NAME, "DOUBLE")

def get_supply_air_fan_amps(event, context):
    return get_data(event, context, TABLE_NAME)