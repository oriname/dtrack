from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from airflow.sensors.time_sensor import TimeSensor
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime, date
import pyodbc
import requests
import json
import random
import logging
import time
from airflow.models import Variable
from config_loader import load_config

# Load configurations
config = load_config()

# Logger setup
logger = logging.getLogger(__name__)

# Database connection details from config
connection_string = config['database']['connection_string']

# Detrack API URL and headers from config
url = config['detrack_api']['url']
headers = {
    'X-API-KEY': config['detrack_api']['api_key'],
    'Content-Type': 'application/json'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['oriname.agbi@tepe.media'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def is_allowed_time():
    current_time = datetime.now().time()
    return current_time >= datetime.strptime("06:00","%H:%M").time() and \
    current_time < datetime.strptime("19:00", "%H:%M").time()

def check_for_new_records():
    if not is_allowed_time():
        return False
    
    logger.info("Checking for new records in the database...")
    connection = None
    try:
        connection = pyodbc.connect(connection_string)
        with connection.cursor() as cursor:
            sql = """
            SELECT TOP 1 1
            FROM dbo.Dtrack_shipping
            WHERE job_status is NULL OR job_status != 'success'
            """
            cursor.execute(sql)
            result = cursor.fetchone()

            # Get the current backoff time
            backoff_time = int(Variable.get("backoff_time", default_var=1))

            if result and result[0] > 0:
                logger.info("New records found.")

                # Reset backoff time if we found a record
                Variable.set("backoff_time", 1)

                return True
            else:
                logger.info("No new records found.")

                # Increase backoff time if no record found
                new_backoff_time = min(backoff_time * 2, 300)  # Cap at 5 minutes
                Variable.set("backoff_time", new_backoff_time)

                return False
    
    except pyodbc.Error as e:
        logger.error(f"Error checking for new records in the database: {e}")
        raise
    finally:
        if connection is not None:
            connection.close()

def fetch_vehicle_ids_from_api():
    logger.info("Fetching vehicle IDs from API...")
    try:
        response = requests.get("https://app.detrack.com/api/v2/vehicles", headers=headers)
        response.raise_for_status()
        vehicles_data = response.json()
        vehicles = vehicles_data.get('data', [])
        vehicle_ids = [vehicle['detrack_id'] for vehicle in vehicles]
        logger.info(f"Fetched vehicle IDs: {vehicles}")
        return vehicle_ids
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch vehicle IDs from API: {e}")
        raise

def fetch_package_items(tracking_number, do_number, connection):
    logger.info(f"Fetching package items for TrackingNumber: {tracking_number}, JobNumber: {do_number}...")
    items = []
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT TotalQtyShipped, Description
                FROM dbo.Package
                WHERE ShipmentNumber = ? AND JobNumber = ?
            """
            cursor.execute(sql, tracking_number, do_number)
            result = cursor.fetchall()
            items = [
                {
                    "quantity": row.TotalQtyShipped,
                    "description": row.Description,
                    "id": None,
                    "sku": None,
                    "purchase_order_number": None,
                    "batch_number": None,
                    "expiry_date": None,
                    "comments": None,
                    "unit_of_measure": None,
                    "checked": False,
                    "actual_quantity": None,
                    "inbound_quantity": None,
                    "unload_time_estimate": None,
                    "unload_time_actual": None,
                    "follow_up_quantity": None,
                    "follow_up_reason": None,
                    "rework_quantity": None,
                    "rework_reason": None,
                    "reject_quantity": 0,
                    "reject_reason": None,
                    "weight": None,
                    "serial_numbers": [],
                    "photo_url": None
                } for row in result
            ]
    except pyodbc.Error as e:
        logger.error(f"Error fetching package items from database: {e}")
        raise
    return items

def fetch_jobs_from_db():
    logger.info("Fetching jobs from database...")
    vehicle_ids = fetch_vehicle_ids_from_api()
    if not vehicle_ids:
        logger.error("No vehicle IDs fetched from API.")
        raise ValueError("No vehicle IDs fetched from API.")
    
    jobs = []
    connection = None
    try:
        connection = pyodbc.connect(connection_string)
        with connection.cursor() as cursor:
            sql = "SELECT * FROM dbo.Dtrack_Shipping WHERE job_status != 'success' OR job_status IS NULL"
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description]
            logger.info(f"Column names in Dtrack_Shipping: {columns}")
            
            result = cursor.fetchall()
            for row in result:
                row_dict = dict(zip(columns, row))
                logger.info(f"Fetched row: {row_dict}")

                items = fetch_package_items(row_dict["tracking_number"], row_dict["do_number"], connection)
                
                job = {
                    "data": {
                        "job_type": row_dict["job_type"],
                        "do_number": row_dict["do_number"],
                        "date": row_dict["delivery_date"].strftime('%Y-%m-%d') if isinstance(row_dict["delivery_date"], (datetime, date)) else row_dict["delivery_date"],
                        "processing_date": row_dict["processing_date"].strftime('%Y-%m-%d') if isinstance(row_dict["processing_date"], (datetime, date)) else row_dict["processing_date"],
                        "delivery_completion_time_window_from": row_dict["delivery_completion_time_window_from"].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row_dict["delivery_completion_time_window_from"], (datetime, date)) else row_dict["delivery_completion_time_window_from"],
                        "delivery_completion_time_window_to": row_dict["delivery_completion_time_window_to"].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row_dict["delivery_completion_time_window_to"], (datetime, date)) else row_dict["delivery_completion_time_window_to"],
                        "tracking_number": row_dict["tracking_number"],
                        "order_number": row_dict["order_number"],
                        "job_sequence": row_dict["job_sequence"],
                        "latitude": row_dict["latitude"],
                        "longitude": row_dict["longitude"],
                        "address": row_dict["address"],
                        "company_name": row_dict["company_name"],
                        "address_1": row_dict["address_1"],
                        "postal_code": row_dict["postal_code"],
                        "city": row_dict["city"],
                        "state": row_dict["state"],
                        "deliver_to_collect_from": row_dict["deliver_to_collect_from"],
                        "phone_number": row_dict["phone_number"],
                        "sender_phone_number": row_dict["sender_phone_number"],
                        "instructions": row_dict["instructions"],
                        "assign_to": random.choice(vehicle_ids),
                        "notify_email": row_dict["notify_email"],
                        "zone": row_dict["zone"],
                        "account_number": row_dict["account_number"],
                        "job_owner": row_dict["job_owner"],
                        "group": row_dict["group"],
                        "weight": row_dict["weight"],
                        "parcel_width": row_dict["parcel_width"],
                        "parcel_length": row_dict["parcel_length"],
                        "parcel_height": row_dict["parcel_height"],
                        "boxes": row_dict["boxes"],
                        "pallets": row_dict["pallets"],
                        "number_of_shipping_labels": row_dict["number_of_shipping_labels"],
                        "attachment_url": row_dict["attachment_url"],
                        "auto_reschedule": row_dict["auto_reschedule"],
                        "depot_name": row_dict["depot_name"],
                        "depot_contact": row_dict["depot_contact"],
                        "run_number": row_dict["run_number"],
                        "remarks": row_dict["remarks"],
                        "service_time": row_dict["service_time"],
                        "sku": row_dict["sku"],
                        "description": row_dict["description"],
                        "quantity": row_dict["quantity"],
                        "id": row_dict["id"],
                        "items": items
                    }
                }
                jobs.append(job)
        logger.info(f"Fetched {len(jobs)} jobs from database.")
    except pyodbc.Error as e:
        logger.error(f"Error fetching jobs from database: {e}")
        raise
    finally:
        if connection is not None:
            connection.close()
    return jobs

def update_job_status(job_id, job_status, error_message=None):
    logger.info(f"Updating job status for job ID {job_id} to {job_status}...")
    try:
        connection = pyodbc.connect(connection_string)
        with connection.cursor() as cursor:
            sql = "UPDATE dbo.Dtrack_Shipping SET job_status = ?, error_message = ? WHERE id = ?"
            cursor.execute(sql, job_status, error_message, job_id)
            connection.commit()
    except pyodbc.Error as e:
        logger.error(f"Error updating job status in database: {e}")
        raise
    finally:
        if connection is not None:
            connection.close()

def process_new_records():
    logger.info("Processing new records...")
    try:
        jobs = fetch_jobs_from_db()
        failed_jobs = []
        for job in jobs:
            try:
                response = requests.post(url, headers=headers, data=json.dumps(job))
                response.raise_for_status()
                job_status = 'success'
                error_message = None
            except requests.exceptions.RequestException as e:
                job_status = 'fail'
                error_message = response.text if response else str(e)
                logger.error(f"Failed to create job via API: {e} and {response.text if response else ''}")
                failed_jobs.append((job["data"]["id"], job_status, error_message))
            update_job_status(job["data"]["id"], job_status, error_message)
        
        if failed_jobs:
            logger.warning(f"Failed to create {len(failed_jobs)} jobs via API. See logs for details")
            raise Exception(f"Failed to create {len(failed_jobs)} jobs via API.")
    except Exception as e:
        logger.error(f"Error processing new records: {e}")
        raise


# Define the Airflow DAG
with DAG(
    'detrack_data_pipeline',
    default_args=default_args,
    description='Data pipeline for Detrack',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    start_date=days_ago(1),
    catchup=False
) as dag:
    
    wait_until_allowed_time = TimeSensor(
        task_id='wait_until_allowed_time',
        target_time=datetime.strptime("06:00", "%H:%M").time(),
        poke_interval=60 * 60,  # Check every hour
    )
    
    # Task to check for new records
    check_new_records = PythonSensor(
        task_id='check_for_new_records',
        python_callable=check_for_new_records,
        timeout=300,  # 5 minutes timeout
        poke_interval=20,  # Start with checking every 30 seconds
        mode='poke',
        exponential_backoff=True,
        max_wait=timedelta(minutes=5),  # Maximum wait time of 5 minutes
        soft_fail=True,  # Don't fail the entire DAG run if this task fails
    )

    process_records = PythonOperator(
        task_id='process_new_records',
        python_callable=process_new_records,
    )

    skip_processing = DummyOperator(
        task_id='skip_processing',
    )

    wait_until_allowed_time >> check_new_records >> [process_records, skip_processing]