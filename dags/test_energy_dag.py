from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pyodbc
import logging
from config_loader import load_config
from math import isnan
from collections import defaultdict

# Get today's date
today = datetime.now().date()

# Load energy configuration
energy_config = load_config(config_file='config/energy_config.json')

# Logger setup
logger = logging.getLogger(__name__)

# Database connection details
connection_string = energy_config['database']['connection_string']

# Constants
VOLTAGE = 200  # Voltage in volts
BATCH_SIZE = 10000  # Number of records to process per batch

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['oriname.agbi@bcu.ac.uk'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

def get_processing_timeframe():
    current_time = datetime.now()
    
    # Round down to the most recent 30-minute interval
    rounded_time = current_time.replace(minute=0 if current_time.minute < 30 else 30, second=0, microsecond=0)
    logger.info(f"Current time: {current_time}, Rounded time: {rounded_time}")
    
    # If it's close to midnight (e.g., between 23:30 and 00:30), do a full run
    if rounded_time.hour == 0 and rounded_time.minute == 0:
        return None  # Indicates a full run
    else:
        # Go back 3 hours from the rounded time
        three_hours_ago = rounded_time - timedelta(hours=3)
        
        return three_hours_ago

def calculate_energy(current, voltage, time_interval_hours):
    """
    Calculate energy consumption in kWh.
    
    :param current: Current in amperes
    :param voltage: Voltage in volts
    :param time_interval_hours: Time interval in hours
    :return: Energy in kWh
    """
    energy = (current * voltage * time_interval_hours) / 1000
    return energy

def round_to_next_half_hour(timestamp):
    """
    Round a datetime object to the next half-hour.
    
    :param timestamp: The datetime object to round.
    :return: The rounded datetime object.
    """
    if timestamp.minute < 30:
        return timestamp.replace(minute=30, second=0, microsecond=0)
    else:
        return timestamp.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

def process_readings():
    """
    Process the energy readings and update the database.
    """
    try:
        with pyodbc.connect(connection_string) as connection:
            cursor = connection.cursor()

            timeframe = get_processing_timeframe()

            if timeframe is None:
                # Full run
                cursor.execute("SELECT * FROM dbo.Data")
                logger.info("Performing full data calculation")
            else:
                # Partial run
                cursor.execute("SELECT * FROM dbo.Data WHERE timestamp >= ?", (timeframe,))
                logger.info(f"Performing partial data calculation from {timeframe}")

            columns = [column[0] for column in cursor.description]
            readings = list(dict(zip(columns, row)) for row in cursor.fetchall())

            energy_by_interval = defaultdict(lambda: {'energy': 0, 'deviceName': ''})
            batch = []
            batch_number = 0

            for reading in readings:
                current1, current2, current3 = map(float, [reading['channel1'], reading['channel2'], reading['channel3']])
                if any(isnan(c) for c in [current1, current2, current3]):
                    continue

                timestamp = reading['timestamp']
                device_name = reading['deviceName']
                # Round timestamp to the next half-hour
                interval_key = round_to_next_half_hour(timestamp)
                interval_key_string = interval_key.isoformat()

                energy_by_interval[(device_name, interval_key_string)]['deviceName'] = device_name
                # Each reading represents a 30-second interval
                time_interval_seconds = 30
                time_interval_hours = time_interval_seconds / 3600

                energy_change = sum(calculate_energy(c, VOLTAGE, time_interval_hours) for c in [current1, current2, current3])
                energy_by_interval[(device_name, interval_key_string)]['energy'] += energy_change

            for key, value in energy_by_interval.items():
                batch.append((value['deviceName'], datetime.fromisoformat(key[1]), value['energy']))

                if len(batch) >= BATCH_SIZE:
                    batch_number += 1
                    insert_batch(cursor, batch, batch_number)
                    batch.clear()

            if batch:
                batch_number += 1
                insert_batch(cursor, batch, batch_number)

            connection.commit()
            logger.info("All batches processed and committed successfully.")

    except Exception as e:
        logger.error(f"Error processing readings: {e}")
        raise

def insert_batch(cursor, batch, batch_number):
    """
    Insert a batch of records into the database.

    :param cursor: Database cursor
    :param batch: List of records to insert
    :param batch_number: The batch number
    """
    try:
        create_temp_table_sql = """
        CREATE TABLE #temp_energy_data (
            DeviceName VARCHAR(255),
            Timestamp DATETIME,
            EnergyConsumed FLOAT
        )
        """
        cursor.execute(create_temp_table_sql)

        insert_sql = "INSERT INTO #temp_energy_data (DeviceName, Timestamp, EnergyConsumed) VALUES (?, ?, ?)"
        cursor.executemany(insert_sql, batch)

        merge_sql = """
        MERGE INTO [iot_data].[dbo].[consumption] AS target
        USING #temp_energy_data AS source
        ON (target.DeviceName = source.DeviceName AND target.Timestamp = source.Timestamp)
        WHEN MATCHED THEN 
            UPDATE SET target.EnergyConsumed = source.EnergyConsumed
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DeviceName, Timestamp, EnergyConsumed)
            VALUES (source.DeviceName, source.Timestamp, source.EnergyConsumed);
        """
        cursor.execute(merge_sql)
        cursor.execute("DROP TABLE #temp_energy_data")
        logger.info(f"Batch {batch_number} processed and merged successfully.")
    except Exception as e:
        logger.error(f"Error processing batch {batch_number}: {e}")
        raise

# Define the Airflow DAG
with DAG(
    'energy_data_pipeline_test',
    default_args=default_args,
    description='Pipeline to process energy data and update the database',
    schedule_interval='5,35 * * * *',  # Cron expression for 5 minutes past each half-hour and hour
    start_date=days_ago(1),
    catchup=False
) as dag:
    
    process_energy_data = PythonOperator(
        task_id='process_energy_data',
        python_callable=process_readings
    )

    process_energy_data
