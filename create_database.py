import boto3
import os

from dotenv import load_dotenv

stage = os.getenv('STAGE', 'develop')
dotenv_path = f'.env.{stage}'
load_dotenv(dotenv_path=dotenv_path)

def create_timestream_database():
    client = boto3.client('timestream-write')

    database_name = os.getenv('DATABASE_NAME')

    try:
        client.create_database(DatabaseName=database_name)
        print(f"Database '{database_name}' created successfully.")
    except client.exceptions.ConflictException:
        print(f"Database '{database_name}' already exists.")
    except Exception as e:
        print(f"Error creating database: {str(e)}")

    # Create tables
    tables = {
        'COMPRESSOR_AMPS': os.getenv('COMPRESSOR_AMPS_TABLE_NAME'),
        'COND_FAN_AMPS': os.getenv('COND_FAN_AMPS_TABLE_NAME'),
        'LIQUID_LINE_PRESSURE': os.getenv('LIQUID_LINE_PRESSURE_TABLE_NAME'),
        'LIQUID_LINE_TEMPERATURE': os.getenv('LIQUID_LINE_TEMPERATURE_TABLE_NAME'),
        'RETURN_AIR_CO2': os.getenv('RETURN_AIR_CO2_TABLE_NAME'),
        'RETURN_AIR_HUMIDITY': os.getenv('RETURN_AIR_HUMIDITY_TABLE_NAME'),
        'RETURN_AIR_TEMPERATURE': os.getenv('RETURN_AIR_TEMPERATURE_TABLE_NAME'),
        'SPACE_CO2': os.getenv('SPACE_CO2_TABLE_NAME'),
        'SPACE_HUMIDITY': os.getenv('SPACE_HUMIDITY_TABLE_NAME'),
        'SPACE_TEMPERATURE': os.getenv('SPACE_TEMPERATURE_TABLE_NAME'),
        'SUCTION_LINE_PRESSURE': os.getenv('SUCTION_LINE_PRESSURE_TABLE_NAME'),
        'SUCTION_LINE_TEMPERATURE': os.getenv('SUCTION_LINE_TEMPERATURE_TABLE_NAME'),
        'SUPPLY_AIR_CO2': os.getenv('SUPPLY_AIR_CO2_TABLE_NAME'),
        'SUPPLY_AIR_FAN_AMPS': os.getenv('SUPPLY_AIR_FAN_AMPS_TABLE_NAME'),
        'SUPPLY_AIR_HUMIDITY': os.getenv('SUPPLY_AIR_HUMIDITY_TABLE_NAME'),
        'SUPPLY_AIR_TEMPERATURE': os.getenv('SUPPLY_AIR_TEMPERATURE_TABLE_NAME'),
        'VIBRATION_SENSOR': os.getenv('VIBRATION_SENSOR_TABLE_NAME'),
        'LOG_DATA': os.getenv('LOG_DATA_TABLE_NAME'),
    }

    for table_name in tables.values():
        try:
            client.create_table(
                DatabaseName=database_name,
                TableName=table_name,
                RetentionProperties={
                    'MemoryStoreRetentionPeriodInHours': 24,
                    'MagneticStoreRetentionPeriodInDays': 7
                }
            )
            print(f"Table '{table_name}' created successfully.")
        except client.exceptions.ConflictException:
            print(f"Table '{table_name}' already exists.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {str(e)}")


if __name__ == "__main__":
    create_timestream_database()
