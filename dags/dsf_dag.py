from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import os
import re
import pyodbc
import shutil
import logging
from config_loader import load_config

# Load configurations
dsf_config = load_config(config_file='config/dsf_config.json')

# Logger setup
logger = logging.getLogger(__name__)

# Database connection details from config
connection_string = dsf_config['database']['connection_string']
source_folder = dsf_config['folder_paths']['source_path']
destination_folder = dsf_config['folder_paths']['destination_path']

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_job_number(order_number):
    try:
        conn = pyodbc.connect(connection_string)  # Correcting the connection
        cursor = conn.cursor()

        # SQL query to find job number based on DSF order number in description
        query = """
        SELECT TOP 1 JobNumber 
        FROM dbo.OrderHeader 
        WHERE JobDescriptionn LIKE ?
        ORDER BY OrderDate DESC
        """
        
        search_pattern = f'%DSF Order #{order_number}%'
        cursor.execute(query, (search_pattern,))
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        if result:
            return result[0]
        return None
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        return None

def process_files(folder_path, **context):
    processed_files = []
    errors = []

    try:
        # Get list of files in the folder
        files = os.listdir(folder_path)

        for file in files:
            try:
                order_number = None
                
                # Extract order number using regex for different patterns
                match_order_prefix = re.search(r'Order(\d+)_', file)
                match_dot_number_dash = re.search(r'\.(\d+)-', file)

                if match_order_prefix:
                    order_number = match_order_prefix.group(1)
                elif match_dot_number_dash:
                    order_number = match_dot_number_dash.group(1)

                if order_number:
                    # Get corresponding job number from database
                    job_number = get_job_number(order_number)

                    if job_number:
                        # Set new filename with .pdf extension
                        new_filename = f"{job_number}.pdf"

                        # Construct full paths
                        old_path = os.path.join(folder_path, file)
                        new_path = os.path.join(destination_folder, new_filename)

                        # Check if destination file already exists
                        if os.path.exists(new_path):
                            error_msg = f"Destination file {new_filename} already exists"
                            logger.error(error_msg)
                            errors.append({"file": file, "error": error_msg})
                            continue

                        # Rename/move file
                        shutil.move(old_path, new_path)
                        processed_files.append({
                            "original": file,
                            "new": new_filename,
                            "job_number": job_number,
                            "order_number": order_number
                        })
                        logger.info(f"Successfully renamed {file} to {new_filename}")
                    else:
                        error_msg = f"No matching job number found for order {order_number}"
                        logger.error(error_msg)
                        errors.append({"file": file, "error": error_msg})

            except Exception as e:
                error_msg = f"Error processing {file}: {str(e)}"
                logger.error(error_msg)
                errors.append({"file": file, "error": error_msg})

    except Exception as e:
        logger.error(f"Error accessing folder: {str(e)}")
        raise

    # Return processing results
    return {
        "processed_files": processed_files,
        "errors": errors,
        "total_processed": len(processed_files),
        "total_errors": len(errors)
    }

# Create DAG
dag = DAG(
    'dsf_file_rename',
    default_args=default_args,
    description='Rename DSF order files to corresponding job numbers using MSSQL',
    schedule_interval=timedelta(minutes=5),  # Runs every 5 minutes, adjust as needed
    catchup=False
)

# Define task
rename_task = PythonOperator(
    task_id='rename_files',
    python_callable=process_files,
    op_kwargs={'folder_path': source_folder},
    dag=dag
)

# Set task dependencies (if you add more tasks later)
rename_task
