from helpers import store_data, get_data


def store_cond_fan_amps(event, context):
    return store_data(event, context, 'CondFanAmps', 'cond_fan_amps', "DOUBLE")

def get_cond_fan_amps(event, context):
    return get_data(event, context, 'CondFanAmps')