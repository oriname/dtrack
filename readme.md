# Changelog and README

## Overview for DeTrack Integration --- DeTrack is a third part delivery app which clarkeprint uses for inhouse delivery. 
## What we did was build on the api provided by DeTrack. the pipeline flow is as follows. a EPMS user booking a shipping datawhen conditions are met
##
##
##
##
##
##

This document outlines the changes and configurations needed for the project, including details on mapping, updating, and replicating data between different tables, setting up error handling, and orchestrating workflows using Airflow. All `V_*` files mentioned are the latest versions.

## Objectives

1. **Clean up code**.
2. **Map tables**:
   - Map `[DTrack].[dbo].[Shipment]` to `[DTrack].[dbo].[Dtrack_Shipping]`.
3. **Trigger updates and replications**:
   - Trigger an update or replicate from `[DTrack].[dbo].[Shipment]` to `[DTrack].[dbo].[Dtrack_Shipping]` when `[ShipVia]` is `1005` and `[isPartial]` is `1`.

## Required Files

1. **Mapping and Updating**:
   - SQL scripts to map and perform updates or inserts from `[DTrack].[dbo].[Shipment]` to `[DTrack].[dbo].[Dtrack_Shipping]`.
2. **API Execution**:
   - A file to run the API to create jobs.
3. **Configuration**:
   - A configuration file to store credentials.
4. **Job Assignment**:
   - A file to auto-assign jobs.

## Technology Stack

1. Docker
2. Airflow
3. Python
4. SQL

## Detailed File Descriptions

### SQL Scripts

1. **CopyShipmentToDTrack.sql, MapToDtrack.sql**:
   - Trigger fires whenever a new record with `ShipVia` value `10005` and `isPartial` value `1` is inserted into the `Enterprise32.dbo.Shipment` table. The same record will be inserted into the `DTrack.dbo.Shipment` table.
2. **Mapping**:
   - `target.[Shipped] = source.[isPartial]`: The `isPartial` column is mapped to the `Shipped` column of the `DTrack.dbo.Shipment` table.
3. **Error Logging**:
   - An `ErrorLog` table should be created to catch all errors from the trigger.

## Changes Implemented

1. **Column Update**:
   - Removed `Shipped` column and replaced it with `isPartial`. This change helps sales reps understand that `isPartial` means an item is shipped, while `Shipped` means an item is delivered.

2. **Error Handling in SQL**:
   - Incorporated `BEGIN TRY...END TRY` and `BEGIN CATCH...END CATCH` blocks to handle errors gracefully during the MERGE operation.
     - **Error Logging Table**: Stores detailed information about errors.
     - **Error Handling**: Main logic within `BEGIN TRY...END TRY`. On error, control passes to `BEGIN CATCH...END CATCH`.
     - **Logging Errors**: Errors are logged in the `ErrorLog` table.
     - **Raising Errors**: Errors are re-thrown using `THROW` to inform the calling application.

3. **Airflow Setup**:
   - Configured Airflow for orchestration and data engineering.
     - **Directory**: `\Data_engine`
     - **Files**:
       - `dtrack_dag.py`: The DAG file.
       - `config.json`: Credentials file.

4. **Improved Error Management in Airflow**:
   - **DAG Updates**:
     - **Job Status**: Updates `job_status` to "fail" if the API rejects data due to validation errors.
   - **API Validation Error Handling**:
     - **Create Jobs Function**:
       - Captures and logs full error response from the API.
       - Updates `job_status` to "fail" if the API returns a validation error or any failure response.
       - Collects failed jobs in a list `failed_jobs`.
       - Raises an exception if there are any failed jobs, ensuring the Airflow task is marked as failed.
       - Logs detailed error messages and updates `job_status`.

## Workflow Summary

## 27/05/2024
Started coding the data piple line for Detrack Delivery Application.
SQL - Created trigger which detects changes in Shipping data and updates the Dtrack table with this changes
	Two triggers where created. The first trigger pulls data from shipment table of Enterprise database to Dtrack shippment table
	The second trigger maps the data in shipment table of DTrack database to the DTrack_Shipping table. This is the table that the API calls happens
	So it is [Enterprise32].[dbo].[Shipment]------> [DTrack].[dbo].[Shipment]------->[DTrack].[dbo].[Dtrack_Shipping]
API - API call [DTrack].[dbo].[Dtrack_Shipping]--------> (API) DeTrack ----> iPad



## 12/06/2024
Error management for the data pipeline - Now if there is a validation error in the API call, for example when the data retrieved from the database is missing a field,
error should be logged and the job retired. This roboustness wasnt available before now.

I have added a near real time for update I added a check_new_records fuction that calls the pythonsensor of airflow
I am not comfortable with this method because if I set schedule_interval=timedelta(days=1) it takes forever to pick new
records but when I set it to schedule_interval=timedelta(minutes=5) it does in a real time even poke_interval=10
A better solution is event driven using triggers this also save resources.


##24/06/2024
I implemented backoff strategy
Here's how this backoff strategy works:

We use Airflow's Variable to store the current backoff time. This allows the backoff time to persist across function calls.
Initially, we set the backoff time to 1 second.
If we find a new record:

We reset the backoff time to 1 second.
We return True to indicate that we found a new record.


If we don't find a new record:

We double the backoff time (exponential backoff).
We cap the backoff time at 5 minutes (300 seconds) to ensure we don't wait too long between checks.
We sleep for the current backoff time before returning False.

This strategy provides several benefits:

When there are frequent new records, checks will happen quickly (every second).
During quiet periods, the time between checks will increase, reducing database load.
The backoff resets as soon as a new record is found, ensuring responsiveness.
The maximum backoff time prevents the system from becoming unresponsive for too long.


08/07/2024 - This is for iOT devices deployed for energy monitoring, 
*designed and eployed pipe line ETL, ELT pipeline for energy consumption. moved away from Node red implementation.Why? I was gettign negative values,
processing 600K records to calculate energy consume every 30mins was throwing some errors. I implemented the new architecture as that of DTrack using Python,
Docker and Airflow.
* I added schedule_interval='5,35 * * * *' to the DAG. To schedule your Airflow DAG to run every 35 minutes, but specifically 5 minutes after each hour and half-hour, you can use a cron expression in the schedule_interval. For instance, it will run at 00:05, 00:35, 01:05, 01:35, ..., 23:05, and 23:35.
* Architecture for Energy consumption is Sensor ----> node funtion which does the ETL----> MSSQL node which helps in inserting the data
** Then second architecture, the stored Data is ingested or extract, transformed or preprocess by calculating the energy consumed for every 30mins and load or inserted into the energy_data table


01/08/2024

** updated Dtrack DAG to make mark process as fail if there is fail issue as against saying it was success but failed in processing jobs