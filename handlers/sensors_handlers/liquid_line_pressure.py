import os

from dotenv import load_dotenv
from handlers.common import store_data, get_data


load_dotenv(dotenv_path='.env')
TABLE_NAME = os.getenv('LIQUID_LINE_PRESSURE_TABLE_NAME')


def store_liquid_line_pressure(event, context):
    return store_data(event, context, TABLE_NAME, TABLE_NAME, "DOUBLE")

def get_liquid_line_pressure(event, context):
    return get_data(event, context, TABLE_NAME)