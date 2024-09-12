from helpers import store_data, get_data



def store_suction_line_temperature(event, context):
    return store_data(event, context, 'SuctionLineTemperature', 'suction_line_temperature', "DOUBLE")

def get_suction_line_temperature(event, context):
    return get_data(event, context, 'SuctionLineTemperature')