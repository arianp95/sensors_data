import boto3
import json
import os
from botocore.config import Config
from botocore.exceptions import ClientError
from functools import wraps
from dotenv import load_dotenv

load_dotenv()
stage = os.environ.get('STAGE', 'develop')
load_dotenv(f'.env.{stage}')

DATABASE_NAME = os.environ.get('DATABASE_NAME', '')
API_KEY = os.environ.get('API_KEY', '')

tables_names = {
    "compressor_amps": os.environ.get('COMPRESSOR_AMPS_TABLE_NAME', ''),
    "cond_fan_amps": os.environ.get('COND_FAN_AMPS_TABLE_NAME', ''),
    "liquid_line_pressure": os.environ.get('LIQUID_LINE_PRESSURE_TABLE_NAME', ''),
    "liquid_line_temperature": os.environ.get('LIQUID_LINE_TEMPERATURE_TABLE_NAME', ''),
    "return_air_co2": os.environ.get('RETURN_AIR_CO2_TABLE_NAME', ''),
    "return_air_humidity": os.environ.get('RETURN_AIR_HUMIDITY_TABLE_NAME', ''),
    "return_air_temperature": os.environ.get('RETURN_AIR_TEMPERATURE_TABLE_NAME', ''),
    "space_co2": os.environ.get('SPACE_CO2_TABLE_NAME', ''),
    "space_humidity": os.environ.get('SPACE_HUMIDITY_TABLE_NAME', ''),
    "space_temperature": os.environ.get('SPACE_TEMPERATURE_TABLE_NAME', ''),
    "suction_line_pressure": os.environ.get('SUCTION_LINE_PRESSURE_TABLE_NAME', ''),
    "suction_line_temperature": os.environ.get('SUCTION_LINE_TEMPERATURE_TABLE_NAME', ''),
    "supply_air_co2": os.environ.get('SUPPLY_AIR_CO2_TABLE_NAME', ''),
    "supply_air_fan_amps": os.environ.get('SUPPLY_AIR_FAN_AMPS_TABLE_NAME', ''),
    "supply_air_humidity": os.environ.get('SUPPLY_AIR_HUMIDITY_TABLE_NAME', ''),
    "supply_air_temperature": os.environ.get('SUPPLY_AIR_TEMPERATURE_TABLE_NAME', ''),
    "vibration_sensor": os.environ.get('VIBRATION_SENSOR_TABLE_NAME', ''),
    "log_data": os.environ.get('LOG_DATA_TABLE_NAME', ''),
}

# Boto3 client configuration
config = Config(read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10})
timestream_write = boto3.client("timestream-write", config=config)
timestream_query = boto3.client("timestream-query", config=config)

headers = {
    "Content-Type": "application/json",
}

def require_api_key(func):
    @wraps(func)
    def decorated(event, context):
        provided_api_key = event.get('headers', {}).get('x-api-key')

        if not provided_api_key or provided_api_key != API_KEY:
            return {
                'statusCode': 403,
                'body': 'Forbidden: Invalid API Key'
            }

        return func(event, context)

    return decorated

def generic_query(table_name):
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{table_name}"
        ORDER BY time DESC
        LIMIT 100
        """

        result = timestream_query.query(QueryString=query)

        data = []
        for row in result["Rows"]:
            item = {}
            for i, value in enumerate(row["Data"]):
                column_info = result["ColumnInfo"][i]
                column_name = column_info["Name"]
                column_type = column_info["Type"]["ScalarType"]

                if "ScalarValue" in value:
                    if column_type == "BIGINT":
                        item[column_name] = int(value["ScalarValue"])
                    elif column_type == "DOUBLE":
                        item[column_name] = float(value["ScalarValue"])
                    else:
                        item[column_name] = value["ScalarValue"]
                else:
                    item[column_name] = None

            data.append(item)

        return {
            "statusCode": 200,
            "body": json.dumps(data),
            "headers": headers,
        }
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]
        return {
            "statusCode": 400,
            "body": json.dumps(f"Query error: {error_code} - {error_message}"),
            "headers": headers,
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}"),
            "headers": headers,
        }

@require_api_key
def get_compressor_amps(event, context):
    return generic_query(tables_names['compressor_amps'])

@require_api_key
def get_cond_fan_amps(event, context):
    return generic_query(tables_names['cond_fan_amps'])

@require_api_key
def get_liquid_line_pressure(event, context):
    return generic_query(tables_names['liquid_line_pressure'])

@require_api_key
def get_liquid_line_temperature(event, context):
    return generic_query(tables_names['liquid_line_temperature'])

@require_api_key
def get_return_air_co2(event, context):
    return generic_query(tables_names['return_air_co2'])

@require_api_key
def get_return_air_humidity(event, context):
    return generic_query(tables_names['return_air_humidity'])

@require_api_key
def get_return_air_temperature(event, context):
    return generic_query(tables_names['return_air_temperature'])

@require_api_key
def get_space_co2(event, context):
    return generic_query(tables_names['space_co2'])

@require_api_key
def get_space_humidity(event, context):
    return generic_query(tables_names['space_humidity'])

@require_api_key
def get_space_temperature(event, context):
    return generic_query(tables_names['space_temperature'])

@require_api_key
def get_suction_line_pressure(event, context):
    return generic_query(tables_names['suction_line_pressure'])

@require_api_key
def get_suction_line_temperature(event, context):
    return generic_query(tables_names['suction_line_temperature'])

@require_api_key
def get_supply_air_co2(event, context):
    return generic_query(tables_names['supply_air_co2'])

@require_api_key
def get_supply_air_fan_amps(event, context):
    return generic_query(tables_names['supply_air_fan_amps'])

@require_api_key
def get_supply_air_humidity(event, context):
    return generic_query(tables_names['supply_air_humidity'])

@require_api_key
def get_supply_air_temperature(event, context):
    return generic_query(tables_names['supply_air_temperature'])

@require_api_key
def get_vibration_sensor(event, context):
    return generic_query(tables_names['vibration_sensor'])

@require_api_key
def get_log_data(event, context):
    return generic_query(tables_names['log_data'])