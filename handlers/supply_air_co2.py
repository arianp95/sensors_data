from helpers import store_data, get_data



def store_supply_air_co2(event, context):
    return store_data(event, context, 'SupplyAirCO2', 'supply_air_co2', "DOUBLE")

def get_supply_air_co2(event, context):
    return get_data(event, context, 'SupplyAirCO2')