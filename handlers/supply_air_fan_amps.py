from helpers import store_data, get_data



def store_supply_air_fan_amps(event, context):
    return store_data(event, context, 'SupplyAirFanAmps', 'supply_air_fan_amps', "DOUBLE")

def get_supply_air_fan_amps(event, context):
    return get_data(event, context, 'SupplyAirFanAmps')