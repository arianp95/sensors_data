from helpers import store_data, get_data

def store_return_air_co2(event, context):
    return store_data(event, context, 'ReturnAirCO2', 'return_air_co2', "DOUBLE")

def get_return_air_co2(event, context):
    return get_data(event, context, 'ReturnAirCO2')