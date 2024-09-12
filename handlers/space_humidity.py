from helpers import store_data, get_data


def store_space_humidity(event, context):
    return store_data(event, context, 'SpaceHumidity', 'space_humidity', "DOUBLE")

def get_space_humidity(event, context):
    return get_data(event, context, 'SpaceHumidity')