from helpers import store_data, get_data


def store_suction_line_pressure(event, context):
    return store_data(event, context, 'SuctionLinePressure', 'suction_line_pressure', "DOUBLE")

def get_suction_line_pressure(event, context):
    return get_data(event, context, 'SuctionLinePressure')