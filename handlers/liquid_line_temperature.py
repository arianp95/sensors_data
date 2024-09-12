from helpers import store_data, get_data


def store_liquid_line_temperature(event, context):
    return store_data(event, context, 'LiquidLineTemperature', 'liquid_line_temperature', "DOUBLE")

def get_liquid_line_temperature(event, context):
    return get_data(event, context, 'LiquidLineTemperature')