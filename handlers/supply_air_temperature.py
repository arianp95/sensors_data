import os

from dotenv import load_dotenv
from helpers import store_data, get_data


load_dotenv(dotenv_path='.env')
TABLE_NAME = os.getenv('SUPPLY_AIR_TEMPERATURE_TABLE_NAME')


def store_supply_air_temperature(event, context):
    return store_data(event, context, 'SupplyAirTemperature', 'supply_air_temperature', "DOUBLE")

def get_supply_air_temperature(event, context):
    return get_data(event, context, 'SupplyAirTemperature')