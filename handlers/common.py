import boto3
import json
import os
import time

from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv


config = Config(
    read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
)

load_dotenv(dotenv_path='.env')
timestream_write = boto3.client("timestream-write", config=config)
timestream_query = boto3.client("timestream-query", config=config)

DATABASE_NAME = os.getenv('DATABASE_NAME')

headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
}


def store_data(event, context, table_name, sensor_name, value_type):
    try:
        body = json.loads(event["body"])
        current_time = int(time.time() * 1000)

        record = {
            "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
            "MeasureName": sensor_name,
            "MeasureValueType": "MULTI",
            "MeasureValues": [
                {"Name": "timestamp", "Value": body["timestamp"], "Type": "VARCHAR"},
                {"Name": "trend_flag", "Value": body["trend_flag"], "Type": "VARCHAR"},
                {"Name":"zone_id", "Value": body["zone_id"], "Type": "VARCHAR"},
                {"Name": "building_id", "Value": body["building_id"], "Type": "VARCHAR"},
                {"Name": "status", "Value": body["status"], "Type": "VARCHAR"},
                {
                    "Name": "value",
                    "Value": str(body["value"]),
                    "Type": value_type,
                },
            ],
            "Time": str(current_time),  # Add this line
        }

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=table_name,
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


def get_data(event, context, table_name):
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{table_name}"
        ORDER BY time DESC
        LIMIT 100
        """

        try:
            result = timestream_query.query(QueryString=query)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            return {
                "statusCode": 400,
                "body": json.dumps(f"Query error: {error_code} - {error_message}"),
                "headers": headers,
            }

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
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error: {str(e)}"),
            "headers": headers,
        }

def options_handler(event, context):
    return {
        "statusCode": 200,
        "headers": headers,
        "body": "",
    }