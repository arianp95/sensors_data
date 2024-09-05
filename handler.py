import json
import boto3
import time
from botocore.config import Config
from botocore.exceptions import ClientError

# Configure Timestream client
config = Config(
    read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
)

timestream_write = boto3.client("timestream-write", config=config)
timestream_query = boto3.client("timestream-query", config=config)

DATABASE_NAME = "SensorDatabase"

TABLE_VIBRATION_NAME = "VibrationData"
TABLE_TEMPERATURE_NAME = "TemperatureData"
TABLE_AMPS_NAME = "AmpsData"
TABLE_STATUS_NAME = "StatusData"
TABLE_WIND_DIRECTION_NAME = "WindDirectionData"
TABLE_LOG_NAME = "LogData"

headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
}


def options_handler(event, context):
    return {
        "statusCode": 200,
        "headers": headers,
        "body": "",
    }


# def check_auth(event):
#     try:
#         auth_header = event['headers']['Authorization']
#         encoded_credentials = auth_header.split(' ')[1]
#         decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
#         username, password = decoded_credentials.split(':')

#         # Replace these with your actual credentials
#         VALID_USERNAME = 'admin'
#         VALID_PASSWORD = 'admin'

#         if username == VALID_USERNAME and password == VALID_PASSWORD:
#             return True
#         else:
#             return False
#     except:
#         return False

def store_vibration_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {"WWW-Authenticate": "Basic realm='Restricted'"},
    #     }
    try:
        body = json.loads(event["body"])
        current_time = int(time.time() * 1000)

        record = {
            "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
            "MeasureName": "vibration_sensor",
            "MeasureValueType": "MULTI",
            "MeasureValues": [
                {"Name": "trend_flag", "Value": body["trend_flag"], "Type": "VARCHAR"},
                {"Name": "status", "Value": body["status"], "Type": "VARCHAR"},
                {
                    "Name": "frequency",
                    "Value": str(body["frequency"]),
                    "Type": "BIGINT",
                },
                {
                    "Name": "acceleration",
                    "Value": str(body["acceleration"]),
                    "Type": "DOUBLE",
                },
                {"Name": "velocity", "Value": str(body["velocity"]), "Type": "DOUBLE"},
            ],
            "Time": str(current_time),
        }

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_VIBRATION_NAME,
                Records=[record],
            )

            # Check for mechanical faults
            frequency = body["frequency"]
            query = f"""
            SELECT frequency, acceleration, velocity
            FROM "{DATABASE_NAME}"."{TABLE_VIBRATION_NAME}"
            ORDER BY time DESC
            LIMIT 5
            """
            result = timestream_query.query(QueryString=query)

            accelerations = []
            velocities = []
            for row in result["Rows"]:
                accelerations.append(float(row["Data"][1]["ScalarValue"]))
                velocities.append(float(row["Data"][2]["ScalarValue"]))

            accelerations.reverse()
            velocities.reverse()

            log_record = None

            if len(velocities) > 1:  # Check if there are at least 2 records
                if 10 <= frequency <= 1000 and all(
                    velocities[i] < velocities[i + 1]
                    for i in range(len(velocities) - 1)
                ):
                    log_record = {
                        "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
                        "MeasureName": "log_data",
                        "MeasureValueType": "MULTI",
                        "MeasureValues": [
                            {"Name": "sensor", "Value": "vibration", "Type": "VARCHAR"},
                            {
                                "Name": "log",
                                "Value": "Mechanical Faults",
                                "Type": "VARCHAR",
                            },
                        ],
                        "Time": str(current_time),
                    }
                elif 500 <= frequency <= 16000 and all(
                    accelerations[i] < accelerations[i + 1]
                    for i in range(len(accelerations) - 1)
                ):
                    log_record = {
                        "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
                        "MeasureName": "log_data",
                        "MeasureValueType": "MULTI",
                        "MeasureValues": [
                            {"Name": "sensor", "Value": "vibration", "Type": "VARCHAR"},
                            {
                                "Name": "log",
                                "Value": "Bearings & Gears",
                                "Type": "VARCHAR",
                            },
                        ],
                        "Time": str(current_time),
                    }

            if log_record:
                timestream_write.write_records(
                    DatabaseName=DATABASE_NAME,
                    TableName=TABLE_LOG_NAME,
                    Records=[log_record],
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


def get_vibration_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {
    #             "WWW-Authenticate": "Basic realm='Restricted'",
    #             "Access-Control-Allow-Origin": "*",
    #         },
    #     }
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{TABLE_VIBRATION_NAME}"
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


def store_temperature_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {"WWW-Authenticate": "Basic realm='Restricted'"},
    #     }
    try:
        body = json.loads(event["body"])
        current_time = int(time.time() * 1000)

        record = {
            "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
            "MeasureName": "temperature_sensor",
            "MeasureValueType": "MULTI",
            "MeasureValues": [
                {"Name": "trend_flag", "Value": body["trend_flag"], "Type": "VARCHAR"},
                {"Name": "status", "Value": body["status"], "Type": "VARCHAR"},
                {
                    "Name": "temperature",
                    "Value": str(body["temperature"]),
                    "Type": "DOUBLE",
                },
            ],
            "Time": str(current_time),
        }

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_TEMPERATURE_NAME,
                Records=[record],
            )

            # Check the last 5 temperature measurements
            query = f"""
            SELECT temperature
            FROM "{DATABASE_NAME}"."{TABLE_TEMPERATURE_NAME}"
            ORDER BY time DESC
            LIMIT 5
            """
            result = timestream_query.query(QueryString=query)

            temperatures = [
                float(row["Data"][0]["ScalarValue"]) for row in result["Rows"]
            ]
            temperatures.reverse()  # Reverse to get chronological order

            if len(temperatures) == 5 and all(
                temperatures[i] < temperatures[i + 1] for i in range(4)
            ):
                # If we have 5 measurements and they are strictly increasing
                log_record = {
                    "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
                    "MeasureName": "log_data",
                    "MeasureValueType": "MULTI",
                    "MeasureValues": [
                        {"Name": "sensor", "Value": "temperature", "Type": "VARCHAR"},
                        {
                            "Name": "log",
                            "Value": "Increasing temperature trend detected",
                            "Type": "VARCHAR",
                        },
                    ],
                    "Time": str(current_time),
                }
                timestream_write.write_records(
                    DatabaseName=DATABASE_NAME,
                    TableName=TABLE_LOG_NAME,
                    Records=[log_record],
                )

            if len(temperatures) == 5 and all(
                temperatures[i] > temperatures[i + 1] for i in range(4)
            ):
                # If we have 5 measurements and they are strictly decreasing
                log_record = {
                    "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
                    "MeasureName": "log_data",
                    "MeasureValueType": "MULTI",
                    "MeasureValues": [
                        {"Name": "sensor", "Value": "temperature", "Type": "VARCHAR"},
                        {
                            "Name": "log",
                            "Value": "Decreas temperature trend detected",
                            "Type": "VARCHAR",
                        },
                    ],
                    "Time": str(current_time),
                }
                timestream_write.write_records(
                    DatabaseName=DATABASE_NAME,
                    TableName=TABLE_LOG_NAME,
                    Records=[log_record],
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


def get_temperature_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {
    #             "WWW-Authenticate": "Basic realm='Restricted'",
    #             "Access-Control-Allow-Origin": "*",
    #         },
    #     }
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{TABLE_TEMPERATURE_NAME}"
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


def store_status_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {"WWW-Authenticate": "Basic realm='Restricted'"},
    #     }
    try:
        body = json.loads(event["body"])
        current_time = int(time.time() * 1000)

        record = {
            "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
            "MeasureName": "status_sensor",  # Add this line
            "MeasureValueType": "MULTI",
            "MeasureValues": [
                {"Name": "trend_flag", "Value": body["trend_flag"], "Type": "VARCHAR"},
                {"Name": "status", "Value": body["status"], "Type": "VARCHAR"},
                {
                    "Name": "value",
                    "Value": str(body["value"]),
                    "Type": "VARCHAR",
                },
            ],
            "Time": str(current_time),  # Add this line
        }

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_STATUS_NAME,
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


def get_status_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {
    #             "WWW-Authenticate": "Basic realm='Restricted'",
    #             "Access-Control-Allow-Origin": "*",
    #         },
    #     }
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{TABLE_STATUS_NAME}"
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


def store_amps_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {"WWW-Authenticate": "Basic realm='Restricted'"},
    #     }
    try:
        body = json.loads(event["body"])
        current_time = int(time.time() * 1000)

        record = {
            "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
            "MeasureName": "amps_sensor",  # Add this line
            "MeasureValueType": "MULTI",
            "MeasureValues": [
                {"Name": "trend_flag", "Value": body["trend_flag"], "Type": "VARCHAR"},
                {"Name": "status", "Value": body["status"], "Type": "VARCHAR"},
                {
                    "Name": "value",
                    "Value": str(body["value"]),
                    "Type": "DOUBLE",
                },
            ],
            "Time": str(current_time),  # Add this line
        }

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_AMPS_NAME,
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


def get_amps_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {
    #             "WWW-Authenticate": "Basic realm='Restricted'",
    #             "Access-Control-Allow-Origin": "*",
    #         },
    #     }
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{TABLE_AMPS_NAME}"
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


def store_wind_direction_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {"WWW-Authenticate": "Basic realm='Restricted'"},
    #     }
    try:
        body = json.loads(event["body"])
        current_time = int(time.time() * 1000)

        record = {
            "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
            "MeasureName": "wind_direction_sensor",  # Add this line
            "MeasureValueType": "MULTI",
            "MeasureValues": [
                {"Name": "trend_flag", "Value": body["trend_flag"], "Type": "VARCHAR"},
                {"Name": "status", "Value": body["status"], "Type": "VARCHAR"},
                {
                    "Name": "value",
                    "Value": str(body["value"]),
                    "Type": "VARCHAR",
                },
            ],
            "Time": str(current_time),  # Add this line
        }

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_WIND_DIRECTION_NAME,
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


def get_wind_direction_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {
    #             "WWW-Authenticate": "Basic realm='Restricted'",
    #             "Access-Control-Allow-Origin": "*",
    #         },
    #     }
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{TABLE_WIND_DIRECTION_NAME}"
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


def store_log_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {"WWW-Authenticate": "Basic realm='Restricted'"},
    #     }
    try:
        body = json.loads(event["body"])
        current_time = int(time.time() * 1000)

        record = {
            "Dimensions": [{"Name": "region", "Value": "us-east-2"}],
            "MeasureName": "log_data",  # Add this line
            "MeasureValueType": "MULTI",
            "MeasureValues": [
                {"Name": "sensor", "Value": body["trend_flag"], "Type": "VARCHAR"},
                {"Name": "log", "Value": body["status"], "Type": "VARCHAR"},
            ],
            "Time": str(current_time),  # Add this line
        }

        try:
            timestream_write.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_LOG_NAME,
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


def get_log_data(event, context):
    # if not check_auth(event):
    #     return {
    #         "statusCode": 401,
    #         "body": json.dumps("Unauthorized"),
    #         "headers": {
    #             "WWW-Authenticate": "Basic realm='Restricted'",
    #             "Access-Control-Allow-Origin": "*",
    #         },
    #     }
    try:
        query = f"""
        SELECT *
        FROM "{DATABASE_NAME}"."{TABLE_LOG_NAME}"
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
