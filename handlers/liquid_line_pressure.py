from helpers import store_data, get_data


def store_liquid_line_pressure(event, context):
    return store_data(event, context, 'LiquidLinePressure', 'liquid_line_pressure', "DOUBLE")

def get_liquid_line_pressure(event, context):
    return get_data(event, context, 'LiquidLinePressure')