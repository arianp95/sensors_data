from helpers import store_data, get_data


def store_return_air_temperature(event, context):
    return store_data(event, context, 'ReturnAirTemperature', 'return_air_temperature', "DOUBLE")

def get_return_air_temperature(event, context):
    return get_data(event, context, 'ReturnAirTemperature')