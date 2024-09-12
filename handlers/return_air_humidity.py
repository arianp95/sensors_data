from helpers import store_data, get_data

def store_return_air_humidity(event, context):
    return store_data(event, context, 'ReturnAirHumidity', 'return_air_humidity', "DOUBLE")

def get_return_air_humidity(event, context):
    return get_data(event, context, 'ReturnAirHumidity')