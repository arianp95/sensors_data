from helpers import get_data


def get_log_data(event, context):
    return get_data(event, context, 'LogData')