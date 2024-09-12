from helpers import store_data, get_data



def store_supply_air_humidity(event, context):
    return store_data(event, context, 'SupplyAirHumidity', 'supply_air_humidity', "DOUBLE")

def get_supply_air_humidity(event, context):
    return get_data(event, context, 'SupplyAirHumidity')