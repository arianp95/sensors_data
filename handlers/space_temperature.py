from helpers import store_data, get_data


def store_space_temperature(event, context):
    return store_data(event, context, 'SpaceTemperature', 'SpaceTemperature', "DOUBLE")

def get_space_temperature(event, context):
    return get_data(event, context, 'SpaceTemperature')