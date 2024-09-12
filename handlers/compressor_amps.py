from helpers import store_data, get_data

def store_compressor_amps(event, context):
    return store_data(event, context, 'CompressorAmps', 'compressor_amps', "DOUBLE")

def get_compressor_amps(event, context):
    return get_data(event, context, 'CompressorAmps')