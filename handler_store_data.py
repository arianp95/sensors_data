import boto3
import json
import os
import re
import time

from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from functools import wraps
from typing import Any, Dict, List

load_dotenv()
stage = os.environ.get('STAGE', 'develop')
load_dotenv(f'.env.{stage}')
API_KEY = os.environ.get('API_KEY', '')

DATABASE_NAME = os.environ.get('DATABASE_NAME', '')

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
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
}

TIMESTAMP_PATTERN = r"\d{2}-[A-Za-z]{3}-\d{2} \d{2}:\d{2}:\d{2} [AP]M EDT"
TREND_FLAGS = {"{start}", "{ }"}
STATUSES = {"{ok}", "{fail}"}
REQUIRED_FIELDS = {"timestamp", "trend_flag", "zone_id", "building_id", "status", "value"}


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

def validate_input(body: Dict[str, Any], value_type: str) -> List[str]:
    errors = []

    for field in REQUIRED_FIELDS:
        if field not in body:
            errors.append(f"Missing required field: {field}")

    if "timestamp" in body and not re.match(TIMESTAMP_PATTERN, body["timestamp"]):
        errors.append("Invalid timestamp format. Expected format: 'DD-MMM-YY HH:MM:SS AM/PM EDT'")

    if "trend_flag" in body and body["trend_flag"] not in TREND_FLAGS:
        errors.append("Invalid trend_flag. Must be '{start}' or '{ }'")

    if "status" in body and body["status"] not in STATUSES:
        errors.append("Invalid status. Must be '{ok}' or '{fail}'")

    if "zone_id" in body and not isinstance(body["zone_id"], str):
        errors.append("zone_id must be a string")

    if "building_id" in body and not isinstance(body["building_id"], str):
        errors.append("building_id must be a string")

    if "value" in body:
        if value_type == "DOUBLE":
            try:
                float(body["value"])
            except ValueError:
                errors.append("value must be a number for this sensor type")
        elif value_type == "VARCHAR" and not isinstance(body["value"], str):
            errors.append("value must be a string for this sensor type")

    return errors

def create_record(body: Dict[str, Any], sensor_name: str, value_type: str) -> Dict[str, Any]:
    current_time = int(time.time() * 1000)

    return {
        "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
        "MeasureName": sensor_name,
        "MeasureValueType": "MULTI",
        "MeasureValues": [
            {"Name": "timestamp", "Value": body["timestamp"], "Type": "VARCHAR"},
            {"Name": "trend_flag", "Value": body["trend_flag"], "Type": "VARCHAR"},
            {"Name": "zone_id", "Value": body["zone_id"], "Type": "VARCHAR"},
            {"Name": "building_id", "Value": body["building_id"], "Type": "VARCHAR"},
            {"Name": "status", "Value": body["status"], "Type": "VARCHAR"},
            {"Name": "value", "Value": str(body["value"]), "Type": value_type},
        ],
        "Time": str(current_time),
    }

def generic_store_data(event, context, table_name, value_type):
    try:
        body = json.loads(event["body"])

        validation_errors = validate_input(body, 'DOUBLE')
        if validation_errors:
            return {
                "statusCode": 400,
                "body": json.dumps({"errors": validation_errors}),
                "headers": headers,
            }

        record = create_record(body, table_name, value_type)

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=tables_names[table_name],
                Records=[record],
            )
            return {"statusCode": 200, "body": json.dumps("Data stored successfully"), "headers": headers}
        except ClientError as e:
            if e.response["Error"]["Code"] == "RejectedRecordsException":
                rejected_records = e.response["RejectedRecords"]
                error_message = f"Some records were rejected. Details: {json.dumps(rejected_records)}"
                return {"statusCode": 400, "body": json.dumps(error_message), "headers": headers}
            else:
                raise

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps(f"Error: {str(e)}"), "headers": headers}

@require_api_key
def store_compressor_amps(event, context):
    return generic_store_data(event, context, 'compressor_amps', 'DOUBLE')

@require_api_key
def store_cond_fan_amps(event, context):
    return generic_store_data(event, context, 'cond_fan_amps', 'DOUBLE')

@require_api_key
def store_liquid_line_pressure(event, context):
    return generic_store_data(event, context, 'liquid_line_pressure', 'DOUBLE')

@require_api_key
def store_liquid_line_temperature(event, context):
    return generic_store_data(event, context, 'liquid_line_temperature', 'DOUBLE')

@require_api_key
def store_return_air_co2(event, context):
    return generic_store_data(event, context, 'return_air_co2', 'DOUBLE')

@require_api_key
def store_return_air_humidity(event, context):
    return generic_store_data(event, context, 'return_air_humidity', 'DOUBLE')

@require_api_key
def store_return_air_temperature(event, context):
    return generic_store_data(event, context, 'return_air_temperature', 'DOUBLE')

@require_api_key
def store_space_co2(event, context):
    return generic_store_data(event, context, 'space_co2', 'DOUBLE')

@require_api_key
def store_space_humidity(event, context):
    return generic_store_data(event, context, 'space_humidity', 'DOUBLE')

@require_api_key
def store_space_temperature(event, context):
    return generic_store_data(event, context, 'space_temperature', 'DOUBLE')

@require_api_key
def store_suction_line_pressure(event, context):
    return generic_store_data(event, context, 'suction_line_pressure', 'DOUBLE')

@require_api_key
def store_suction_line_temperature(event, context):
    return generic_store_data(event, context, 'suction_line_temperature', 'DOUBLE')

@require_api_key
def store_supply_air_co2(event, context):
    return generic_store_data(event, context, 'supply_air_co2', 'DOUBLE')

@require_api_key
def store_supply_air_fan_amps(event, context):
    return generic_store_data(event, context, 'supply_air_fan_amps', 'DOUBLE')

@require_api_key
def store_supply_air_humidity(event, context):
    return generic_store_data(event, context, 'supply_air_humidity', 'DOUBLE')

@require_api_key
def store_supply_air_temperature(event, context):
    return generic_store_data(event, context, 'supply_air_temperature', 'DOUBLE')

@require_api_key
def store_vibration_sensor(event, context):
    return generic_store_data(event, context, 'vibration_sensor', 'DOUBLE')

@require_api_key
def store_log_data(event, context):
    return generic_store_data(event, context, 'log_data', 'VARCHAR')

@require_api_key
def options_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return {
        "statusCode": 200,
        "headers": headers,
        "body": "",
    }