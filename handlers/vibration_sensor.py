from helpers import store_data, get_data



def store_vibration_sensor(event, context):
    return store_data(event, context, 'VibrationSensor', 'vibration_sensor', "DOUBLE")

def get_vibration_sensor(event, context):
    return get_data(event, context, 'VibrationSensor')