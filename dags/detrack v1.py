from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
vehicles_url = config['detrack_api']['vehicles_url']  # For fetching vehicle IDs
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


# Check if the current time is within the allowed time range
def is_allowed_time():
    current_time = datetime.now().time()
    return current_time >= datetime.strptime("06:00","%H:%M").time() and \
    current_time < datetime.strptime("23:00", "%H:%M").time()

# check for jobs from the database
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
            # Convert backoff_time to float first, then cast to int
            backoff_time = float(Variable.get("backoff_time", default_var=1))
            backoff_time = int(backoff_time)  # Ensure it's an integer

            if result and result[0] > 0:
                logger.info("New records found.")

                # Reset backoff time if we found a record
                Variable.set("backoff_time", 1)

                return True
            else:
                logger.info("No new records found.")

                # Increase backoff time if no record found
                new_backoff_time = min(int(backoff_time * (random.uniform(1.2, 2.0))), 300)
                Variable.set("backoff_time", new_backoff_time)

                return False
    
    except pyodbc.Error as e:
        logger.error(f"Error checking for new records in the database: {e}")
        raise
    finally:
        if connection is not None:
            connection.close()


# Fetch vehicle IDs from the API
def fetch_vehicle_ids_from_api():
    logger.info("Fetching vehicle IDs from API...")
    try:
        response = requests.get(vehicles_url, headers=headers)
        response.raise_for_status()
        vehicles_data = response.json()
        vehicles = vehicles_data.get('data', [])
        vehicle_ids = [vehicle['detrack_id'] for vehicle in vehicles]
        logger.info(f"Fetched vehicle IDs: {vehicles}")
        return vehicle_ids
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch vehicle IDs from API: {e}")
        raise

# Fetch package items from the database
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
                } for row in result if row.TotalQtyShipped > 0  # Skip items with TotalQtyShipped <= 0
            ]
    except pyodbc.Error as e:
        logger.error(f"Error fetching package items from database: {e}")
        raise
    if not items:
        logger.warning(f"No valid items found for TrackingNumber: {tracking_number}, JobNumber: {do_number}. Skipping this job.")
    
    return items


# Fetch jobs from the database
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
            # SQL query to get all jobs that haven't been marked as 'success'
            sql = "SELECT * FROM dbo.Dtrack_Shipping WHERE job_status != 'success' OR job_status IS NULL"
            cursor.execute(sql)
            columns = [column[0] for column in cursor.description]
            result = cursor.fetchall()
            
            # Logging the number of fetched rows
            logger.info(f"Found {len(result)} rows with job_status != 'success' or job_status IS NULL")
            
            for row in result:
                row_dict = dict(zip(columns, row))
                logger.info(f"Fetched row: {row_dict}")

                # Fetch shipment items (removed check for PackageID, as we're processing shipment-level)
                items = fetch_package_items(row_dict["tracking_number"], row_dict["do_number"], connection)

                if not items:
                    logger.warning(f"No valid shipment items found for TrackingNumber: {row_dict['tracking_number']}, JobNumber: {row_dict['do_number']}. Skipping this job.")
                    continue  # Skip this job if no valid items are found

                # Proceed to create the job structure for valid jobs
                job = {
                    "data": {
                        "job_type": row_dict["job_type"],
                        "do_number": row_dict["DetrackJobNumber"],  # Use DetrackJobNumber for the API call
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
                        "items": items  # Valid shipment items are included
                    }
                }
                jobs.append(job)  # Add only jobs with valid shipment items

        logger.info(f"Fetched {len(jobs)} jobs from database.")
    except pyodbc.Error as e:
        logger.error(f"Error fetching jobs from database: {e}")
        raise
    finally:
        if connection is not None:
            connection.close()
    return jobs


# Update job status in the database
def update_job_status(job_id, job_status, error_message=None):
    logger.info(f"Updating job status for job ID {job_id} to {job_status}...")

    try:
        connection = pyodbc.connect(connection_string)
        with connection.cursor() as cursor:
            # Truncate error_message if it's too long (assuming 255 is the column limit)
            max_error_message_length = 255  # Adjust based on the actual column limit
            truncated_error_message = error_message[:max_error_message_length] if error_message else None

            sql = "UPDATE dbo.Dtrack_Shipping SET job_status = ?, error_message = ? WHERE id = ?"
            cursor.execute(sql, job_status, truncated_error_message, job_id)
            connection.commit()
            logger.info(f"Job {job_id} status updated to {job_status}.")

    except pyodbc.Error as e:
        logger.error(f"Error updating job status in database: {e}")
        raise
    finally:
        if connection is not None:
            connection.close()


    
# Fetch job status from Detrack API
def fetch_job_status_from_api(do_number, delivery_date):
    """
    Helper function to handle the logic of fetching job status from Detrack API.
    This function handles retries with different date parameters and finally tries with no date.
    """
    try:
        # Attempt to fetch job status with delivery_date
        logger.info(f"Attempting API request with delivery date: {delivery_date}")
        response = requests.get(f"{url}/{do_number}?date={delivery_date}", headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error fetching job status for do_number {do_number} with delivery_date {delivery_date}: {e}")
        
        # Fallback to one day earlier
        fallback_date = (datetime.strptime(delivery_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        try:
            logger.info(f"Attempting fallback API request with fallback date: {fallback_date}")
            response = requests.get(f"{url}/{do_number}?date={fallback_date}", headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e2:
            logger.warning(f"Error fetching job status for do_number {do_number} with fallback date {fallback_date}: {e2}")
            
            # Final fallback attempt: Try without date parameter
            try:
                logger.info(f"Attempting final fallback API request without date")
                response = requests.get(f"{url}/{do_number}", headers=headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e3:
                logger.error(f"Final fallback failed for job {do_number}: {e3}")
                raise  # Re-raise the last exception to handle in the calling function


# Update Detrack status for jobs
def update_detrack_status():
    logger.info("Updating Detrack status and Shipped column for jobs...")

    try:
        connection = pyodbc.connect(connection_string)
        with connection.cursor() as cursor:
            # Fetch jobs that need status updates
            sql = """
            SELECT id, DetrackJobNumber, delivery_date, Detrack_Status
            FROM dbo.Dtrack_Shipping
            WHERE job_status = 'success' AND (Detrack_Status IS NULL OR Detrack_Status NOT IN ('completed', 'failed'))
            """
            cursor.execute(sql)
            jobs_to_update = cursor.fetchall()

            for job in jobs_to_update:
                job_id = job.id
                detrack_job_number = job.DetrackJobNumber  # Use DetrackJobNumber from the database
                current_detrack_status = job.Detrack_Status
                delivery_date = job.delivery_date.strftime('%Y-%m-%d')  # Format date to 'YYYY-MM-DD'

                try:
                    # Fetch job status using DetrackJobNumber as do_number in the API
                    status_data = fetch_job_status_from_api(detrack_job_number, delivery_date)
                    logger.info(f"API response for DetrackJobNumber {detrack_job_number}: {status_data}")

                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to fetch job status for DetrackJobNumber {detrack_job_number}: {e}")
                    continue  # Skip to the next job if all attempts fail

                # Extract the status from primary_job_status or latest milestone
                detrack_status = status_data["data"].get("primary_job_status", "Unknown")

                # Skip update if the Detrack_Status hasn't changed
                if detrack_status == current_detrack_status:
                    logger.info(f"No status change for DetrackJobNumber {detrack_job_number}. Skipping update.")
                    continue

                # Optionally, check milestones if necessary
                milestones = status_data["data"].get("milestones", [])
                if milestones:
                    latest_milestone_status = milestones[-1].get("status", "Unknown")
                    logger.info(f"Milestone status for DetrackJobNumber {detrack_job_number}: {latest_milestone_status}")

                # Update the Detrack_Status in the database only if it has changed
                update_sql = "UPDATE dbo.Dtrack_Shipping SET Detrack_Status = ? WHERE id = ?"
                cursor.execute(update_sql, detrack_status, job_id)
                connection.commit()

                logger.info(f"Updated Detrack_Status for job {job_id} to {detrack_status}")

    except pyodbc.Error as e:
        logger.error(f"Error updating Detrack_Status in the database: {e}")
        raise

    finally:
        if connection:
            connection.close()




# Commented out the check_skip_conditions function as per the request

# Function to check for skip conditions based on Detrack_Status, Shipped, and isPartial
# def check_skip_conditions(**kwargs):
#     logger.info("Checking if we should skip processing...")
#
#     try:
#         connection = pyodbc.connect(connection_string)
#         cursor = connection.cursor()
#
#         # SQL to check if there are records that meet the skip conditions
#         sql = """
#         SELECT TOP 1 1
#         FROM dbo.Dtrack_Shipping
#         WHERE job_status = 'success'
#         AND Detrack_Status = 'completed'
#         AND Shipped = 1
#         AND isPartial = 1
#         """
#         cursor.execute(sql)
#         result = cursor.fetchone()
#
#         if result:
#             logger.info("Skipping processing as skip conditions are met.")
#             return 'skip_processing'  # Skip processing if conditions are met
#         else:
#             logger.info("Processing records as skip conditions are not met.")
#             return 'process_records'  # Proceed with processing if conditions are not met
#
#     except pyodbc.Error as e:
#         logger.error(f"Error checking for skip conditions: {e}")
#         raise
#     finally:
#         if connection:
#             connection.close()



# Check if there are valid items for the new records
def check_record_items():
    """
    This function checks if there are valid PackageID(s) and corresponding Quantities for all do_number (JobNumbers) in the Dtrack_Shipping table.
    If a JobNumber has multiple PackageIDs, it ensures all PackageIDs have valid quantities.
    """
    logger.info("Checking items for records...")
    
    connection = None
    valid_job_numbers = []
    invalid_job_numbers = []
    try:
        connection = pyodbc.connect(connection_string)
        with connection.cursor() as cursor:
            # First, fetch all do_number from the Dtrack_Shipping table
            job_sql = """
                SELECT DISTINCT do_number
                FROM dbo.Dtrack_Shipping
                WHERE job_status IS NULL OR job_status != 'success'
            """
            cursor.execute(job_sql)
            job_numbers = cursor.fetchall()
            
            if not job_numbers:
                logger.info("No job numbers found that need item checking.")
                return False
            
            # For each do_number, check associated PackageID and TotalQtyShipped in the Package table
            for job_number in job_numbers:
                do_number = job_number[0]  # Extract the actual do_number from the result tuple
                logger.info(f"Checking items for do_number: {do_number}...")

                package_sql = """
                    SELECT PackageID, TotalQtyShipped
                    FROM dbo.Package
                    WHERE JobNumber = ?
                """
                cursor.execute(package_sql, do_number)
                package_items = cursor.fetchall()
                
                if not package_items:
                    logger.warning(f"No items found for do_number: {do_number}. Skipping...")
                    invalid_job_numbers.append(do_number)
                    continue
                
                # Check that all package items have valid quantities
                invalid_items = [item for item in package_items if item.TotalQtyShipped <= 0]
                
                if invalid_items:
                    logger.warning(f"Invalid items found for do_number: {do_number}. Skipping...")
                    invalid_job_numbers.append(do_number)
                else:
                    logger.info(f"Valid items found for do_number: {do_number}. Proceeding.")
                    valid_job_numbers.append(do_number)
            
            logger.info(f"Valid jobs: {valid_job_numbers}")
            logger.warning(f"Invalid jobs skipped: {invalid_job_numbers}")
            
            return valid_job_numbers  # Return only valid jobs
    
    except pyodbc.Error as e:
        logger.error(f"Error checking items from database: {e}")
        raise
    finally:
        if connection:
            connection.close()

# Process new records
def process_new_records():
    logger.info("Processing new records...")

    try:
        logger.info(f"Using API URL: {url}")

        if not url.startswith(('http://', 'https://')):
            raise ValueError(f"API URL is missing 'http://' or 'https://' protocol: {url}")

        jobs = fetch_jobs_from_db()

        failed_jobs = []
        for job in jobs:
            error_message = None  # Initialize error_message to None for each job
            try:
                logger.info(f"Sending job to API: {job}")

                # Introduce a 1-second delay between each request to avoid exceeding the rate limit
                time.sleep(2)

                response = requests.post(url, headers=headers, data=json.dumps(job), timeout=10)

                if response.status_code in [200, 201]:
                    api_response = response.json()
                    logger.info(f"API response: {api_response}")

                    # Check if an ID exists in the response, indicating job creation
                    if 'id' in api_response.get('data', {}):
                        job_status = 'success'
                        logger.info(f"Job {job['data']['id']} processed successfully with ID {api_response['data']['id']}.")
                    else:
                        job_status = 'fail'
                        error_message = f"Unexpected response content: {api_response}"
                        logger.error(error_message)
                else:
                    # Handle client errors based on status codes
                    job_status = 'fail'
                    if response.status_code == 400:
                        error_message = "Bad request: Malformed JSON or invalid data."
                    elif response.status_code == 401:
                        error_message = "Unauthorized: Check your API key."
                    elif response.status_code == 403:
                        error_message = "Forbidden: You do not have permission."
                    elif response.status_code == 404:
                        error_message = "Record not found."
                    elif response.status_code == 422:
                        error_message = f"HTTP {response.status_code}: {response.text}"
                    elif response.status_code == 429:
                        error_message = "Rate limit exceeded: Retry later."
                        retry_after = response.headers.get('Retry-After', 60)  # Default to 60 seconds if Retry-After header is missing
                        logger.warning(f"Rate limit exceeded, retrying after {retry_after} seconds...")

                        # Wait for the specified retry time before retrying

                        # Check for 'Retry-After' header, with a default fallback if the header is missing
                        retry_after = response.headers.get('Retry-After', 60)  # Default to 60 seconds if 'Retry-After' is missing
                        logger.warning(f"Rate limit exceeded, retrying after {retry_after} seconds...")

                        #wait for the specified retry time before retrying
                        time.sleep(random.uniform(1, int(retry_after)))
                        continue  # Retry after the delay
                    else:
                        error_message = f"Unhandled client error: {response.status_code}"
                    logger.error(f"Job {job['data']['id']} failed with error: {error_message}")

            except requests.exceptions.RequestException as e:
                job_status = 'fail'
                error_message = str(e)
                logger.error(f"Job {job['data']['id']} failed due to request exception: {e}")
                failed_jobs.append((job["data"]["id"], job_status, error_message))

            # Update the job status in the database
            update_job_status(job["data"]["id"], job_status, error_message)

        if failed_jobs:
            logger.warning(f"Failed to create {len(failed_jobs)} jobs via API. See logs for details.")
            raise Exception(f"Failed to create {len(failed_jobs)} jobs via API.")

    except Exception as e:
        logger.error(f"Error processing new records: {e}")
        raise


# Define the function that checks if any status updates are needed
def check_if_status_updates_needed():
    logger.info("Checking if any jobs need status updates...")

    connection = None
    try:
        connection = pyodbc.connect(connection_string)
        with connection.cursor() as cursor:
            # Query to check if there are any jobs with a status that needs updating
            sql = """
            SELECT TOP 1 1
            FROM dbo.Dtrack_Shipping
            WHERE job_status = 'success' AND (Detrack_Status IS NULL OR Detrack_Status NOT IN ('completed', 'failed'))
            """
            cursor.execute(sql)
            result = cursor.fetchone()

            # If there are any jobs that need updating, return True
            if result:
                logger.info("Jobs found that need status updates.")
                return True
            else:
                logger.info("No jobs found that need status updates.")
                return False

    except pyodbc.Error as e:
        logger.error(f"Error checking for status updates: {e}")
        raise
    finally:
        if connection:
            connection.close()


# Define the skip task using DummyOperator
skip_task = DummyOperator(
    task_id='skip_task'
)

def branch_task(**kwargs):
    # Call the check_for_new_records function directly or use XCom to pass results
    new_records_found = check_for_new_records()  # Or retrieve from XCom if needed
    
    if new_records_found:
        logger.info("New records found. Proceeding to check items.")
        return 'check_record_items'
    else:
        logger.info("No new records found. Skipping processing.")
        return 'skip_task'  # Explicitly skip processing if no records
  

# Define the Airflow DAG
with DAG(
    'detrack_data_pipeline',
    default_args=default_args,
    description='Data pipeline for Detrack',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    start_date=days_ago(1),
    catchup=False
) as dag:

    # Wait until the allowed time before proceeding
    wait_until_allowed_time = TimeSensor(
        task_id='wait_until_allowed_time',
        target_time=datetime.strptime("06:00", "%H:%M").time(),
        poke_interval=60 * 60,  # Check every hour
    )

    # Check if there are new records to process
    check_db_for_new_records = PythonSensor(
        task_id='check_for_new_records',
        python_callable=check_for_new_records,
        timeout=300,  # 5 minutes timeout
        poke_interval=60,  # Start with checking every 30 seconds
        mode='poke',
        soft_fail=True,  # Soft fail if no new records are found
    )
    # Branch to check if there are new records to process
    branch = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_task,
)

    # Check if there are valid items for the new records
    check_items = PythonOperator(
        task_id='check_record_items',
        python_callable=check_record_items,  # Function that retries if no items
        retries=5,  # Retry up to 5 times if no valid items are found
        retry_delay=timedelta(minutes=5),
    )

    # Process the records if valid items are found
    process_records = PythonOperator(
        task_id='process_new_records',
        python_callable=process_new_records,
    )

    # Sensor to check if there are pending status updates
    check_for_status_updates = PythonSensor(
        task_id='check_for_status_updates',
        python_callable=check_if_status_updates_needed,  # Calls the function that checks for pending updates
        timeout=300,  # 5 minutes timeout
        poke_interval=20,  # Check every 20 seconds
        mode='poke',
        soft_fail=False,  # Fail the DAG if no status updates are needed after timeout
    )

    # Independent task for updating Detrack status (No schedule_interval here)
    update_detrack_status_task = PythonOperator(
        task_id='update_detrack_status',
        python_callable=update_detrack_status,
    )

# DAG flow for checking and processing records
    # 1. Wait until allowed time
    # 2. Check for new records
    # 3. If records are found, check for items, otherwise skip
    wait_until_allowed_time >> check_db_for_new_records >> branch
    branch >> check_items >> process_records
    branch >> skip_task

    # Independent Detrack update task (can be manually triggered or run on the DAG's schedule)
    check_for_status_updates >> update_detrack_status_task






