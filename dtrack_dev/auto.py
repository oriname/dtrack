import csv
import requests
import json

# Define the URL and headers for creating jobs
url = 'https://app.detrack.com/api/v2/dn/jobs'
headers = {
    'X-API-KEY': 'U79cc5eaef31da24e842af11dc92892615c3abf3ba69850f9',
    'Content-Type': 'application/json'
}

# Function to read CSV and return job data
def read_csv(file_path):
    jobs = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            job = {
                "data": {
                    "do_number": row["Delivery Order (D.O.) No."],
                    "date": row["Delivery Date"],
                    "tracking_number": row["Tracking No."],
                    "order_number": row["Order No."],
                    "address": row["Address"],
                    "postal_code": row["Postal Code"],
                    "deliver_to_collect_from": row["First Name"] + " " + row["Last Name"],
                    "phone_number": row["Customer's Phone No."],
                    "notify_email": row["Emails For Notifications"],
                    "weight": row["Weight"],
                    "parcel_width": row["Width"],
                    "parcel_length": row["Length"],
                    "parcel_height": row["Height"],
                    "items": [
                        {
                            "sku": row["SKU"],
                            "description": row["Description"],
                            "quantity": row["Qty"]
                        }
                    ]
                }
            }
            jobs.append(job)
    return jobs

# Function to create jobs
def create_jobs(jobs):
    for job in jobs:
        response = requests.post(url, headers=headers, data=json.dumps(job))
        print(f'Status Code: {response.status_code}')
        print(f'Response Text: {response.text}')
        if response.status_code != 200:
            print(f'Failed Request Data: {job}')

# Read jobs from CSV
jobs = read_csv('dt.csv')

# Create jobs via API
create_jobs(jobs)
