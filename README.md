# <img width="40" alt="image" src="https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/c879fbf8-1d88-4080-97c3-5dcbd42e240f"> GCP Data Engineering Project: Data Pipeline with Cloud Functions, Airflow and BigQuery ML 🚢

This project will demonstrate how to build a data pipeline on Google Cloud using an event-driven architecture, leveraging services like GCS, Cloud Run functions, and BigQuery. We'll explore both VM and Composer options for managing Airflow, and utilize Logging and Monitoring services to track the pipeline's health. Finally, we'll experiment with BigQuery ML, showcasing how SQL knowledge alone can be sufficient for initial ML implementation in certain scenarios.

![20240823_224651](https://github.com/user-attachments/assets/191c3749-ccc2-4431-aa6e-8f5d691f9abe)

🗃️ **Cloud Storage**: A highly scalable and durable object storage service for storing data of all types, including images, videos, and data files. In this project, we'll use GCS to store CSV files.

⚡ **Cloud Run functions**: A serverless platform for running code in response to events, such as file uploads or database changes.

💽 **Compute Engine**: A virtual machine (VM) service for running custom applications on Google Cloud's infrastructure. In this project, we'll use Compute Engine to host Airflow on a VM.

✨ **Cloud Composer**: A fully managed service for running Apache Airflow, a popular open-source workflow orchestration platform.

🧮 **BigQuery**: A fully managed, serverless data warehouse for storing, querying, and analyzing large datasets. 

🧾 **Cloud Logging**: A fully managed, real-time log management service that provides storage, search, analysis, and alerting for log data at exabyte scale.

📈 **Cloud Monitoring**: A service for gaining visibility into the performance, availability, and health of your applications and infrastructure.

🤖 **BigQuery ML**: A powerful tool for building and deploying machine learning models directly within BigQuery, Google's data warehouse.

----


This project was born from the harsh reality that, while I'm still eager to build data engineering projects with Airflow on GCP, Composer 2 is no longer available for free. To address this, I've outlined two options for completing the project:

**Option 1: Airflow on a VM.** This is the most cost-effective approach. You can even complete it using the GCP Free Trial account (which provides $300 for 90 days). While it requires a bit more manual setup, I'll guide you through each step.

**Option 2: Cloud Composer.** I tested this solution with both Composer 2 and the preview release of Composer 3. Unfortunately, Google's recent policy change (effective [May 11th](https://cloud.google.com/composer/docs/release-notes#May_11_2024)) means you can't create Composer 2 environment using a Free Trial account. The minimum requirement for even the Small Composer is 600GB of Persistent Disk SSD, which exceeds the Free Trial quota of 500GB. To use this option with Composer 2, you'll need to activate your GCP account.

![Screenshot (1912)2](https://github.com/user-attachments/assets/388e47a5-bcab-468a-87bc-97e30bce10d9)

---

For this project, I'm using the PortWatch IMF [Daily Port Activity Data and Trade Estimates](https://portwatch.imf.org/datasets/75619cb86e5f4beeb7dab9629d861acf/about) dataset. This dataset provides daily counts of port calls, as well as estimates of import and export volumes (in metric tons) for 1642 ports worldwide. [PortWatch](https://portwatch.imf.org/pages/about) is a collaborative effort between the [International Monetary Fund](https://www.imf.org/en/Home) and the [Environmental Change Institute](https://www.eci.ox.ac.uk/) at the University of Oxford. It leverages real-time data on vessel movements, specifically Automatic Identification System (AIS) signals, as its primary data source. As of July, I was able to download a table containing over 3.3 million records. The data is publicly available and updated weekly. Here are the columns:

- **date**: all port call dates are expressed in UTC.

- **year**: as extracted from date.

- **month**: month 1–12 extracted from date.

- **day**: day 1–31 extracted from date.

- **portid**: port id. Full list of ports and associated additional information can be found [here](https://portwatch.imf.org/datasets/acc668d199d1472abaaf2467133d4ca4/about).

- **portname**: port name.

- **country**: country the port resides in.

- **ISO3**: ISO 3-letter country code of the port.

- **portcalls_container**: number of container ships entering the port at this date.

- **portcalls_dry_bulk**: number of dry bulk carriers entering the port at this date.

- **portcalls_general_cargo**: number of general cargo ships entering the port at this date.

- **portcalls_roro**: number of ro-ro ships entering the port at this date.

- **portcalls_tanker**: number of tankers entering the port at this date.

- **portcalls_cargo**: total number of ships (excluding tankers) entering the port at this date. This is the sum of portcalls_container, portcalls_dry_bulk, portcalls_general_cargo and portcalls_roro.

- **portcalls**: total number of ships entering the port at this date. This is the sum of portcalls_container, portcalls_dry_bulk, portcalls_general_cargo, portcalls_roro and portcalls_tanker.

- **import_container**: total import volume (in metric tons) of all container ships entering the port at this date.

- **import_dry_bulk**: total import volume (in metric tons) of all dry bulk carriers entering the port at this date.

- **import_general_cargo**: total import volume (in metric tons) of all general cargo ships entering the port at this date.

- **import_roro**: total import volume (in metric tons) of all ro-ro ships entering the port at this date.

- **import_tanker**: total import volume (in metric tons) of all tankers entering the port at this date.

- **import_cargo**: total import volume (in metric tons) of all ships (excluding tankers) entering the port at this date. This is the sum of import_container, import_dry_bulk, import_general_cargo and import_roro.

- **import**: total import volume (in metric tons) of all ships entering the port at this date. This is the sum of import_container, import_dry_bulk, import_general_cargo, import_roro and import_tanker.

- **export_container**: total export volume (in metric tons) of all container ships entering the port at this date.

- **export_dry_bulk**: total export volume (in metric tons) of all dry bulk carriers entering the port at this date.

- **export_general_cargo**: total export volume (in metric tons) of all general cargo ships entering the port at this date.

- **export_roro**: total export volume (in metric tons) of all ro-ro ships entering the port at this date.

- **export_tanker**: total export volume (in metric tons) of all tankers entering the port at this date.

- **export_cargo**: total export volume (in metric tons) of all ships (excluding tankers) entering the port at this date. This is the sum of export_container, export_dry_bulk, export_general_cargo and export_roro.

- **export**: total export volume (in metric tons) of all ships entering the port at this date. This is the sum of export_container, export_dry_bulk, export_general_cargo, export_roro and export_tanker.

Here's what the data looks like:

```sql
date,year,month,day,portid,portname,country,ISO3,portcalls_container,portcalls_dry_bulk,portcalls_general_cargo,portcalls_roro,portcalls_tanker,portcalls_cargo,portcalls,import_container,import_dry_bulk,import_general_cargo,import_roro,import_tanker,import_cargo,import,export_container,export_dry_bulk,export_general_cargo,export_roro,export_tanker,export_cargo,export,ObjectId
2019/07/20 00:00:00+00,2019,7,20,port0,Abbot Point,Australia,AUS,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1
2019/07/21 00:00:00+00,2019,7,21,port0,Abbot Point,Australia,AUS,0,1,0,0,0,1,1,0,0,0,0,0,0,0,0,50243.6185004039,0,0,0,50243.6185004039,50243.6185004039,2
```

To simulate a real-world scenario, I will divide the data file into smaller chunks based on the year (e.g., Daily_Port_Activity_Data_and_Trade_Estimates_2019.csv; in reality, you would likely receive these files daily or hourly from the API). If you wish to partition this or any other data, use this code.

```python
import csv

input_file = r'C:\path-to-your-file\Daily_Port_Activity_Data_and_Trade_Estimates.csv'  #Path to the input CSV file
output_file = r'C:\path-to-your-file\output.csv' #Path to the output CSV file

#Open input and output files
with open(input_file, 'r') as file_in, open(output_file, 'w', newline='') as file_out:
    reader = csv.DictReader(file_in)
    writer = csv.DictWriter(file_out, fieldnames=reader.fieldnames)
    writer.writeheader()

    #Iterate over rows and write the filtered rows to the output file
    for row in reader:
        if row['year'] == '2024':
            writer.writerow(row)

print('Filtered rows saved to', output_file)
```

All data chunks will share the same main header.

```sql
date,year,month,day,portid,portname,country,ISO3,portcalls_container, etc.
```

The data spans from 2019 to 2023, covering all 12 months each year. However, the 2024 data only includes information up to July 7th. This distinction will be important when we transition to the BigQuery ML part.

# 💽 Airflow on GCP VM

Let's start by installing Airflow on VM. Go to Compute Engine and create your VM. I'm using the n2-standard-2 machine type for this setup. Add the OAuth scopes for your VM. You can find more information about this here. My VM's Cloud API access scopes will be set to "Allow full access to all Cloud APIs". While this provides maximum flexibility, it's important to remember that for security best practices, limiting access to essential services is always recommended. If you miss this step, you'll likely encounter `google.api_core.exceptions.Forbidden: 403 Access Denied: BigQuery BigQuery: Missing required OAuth scope` error later. Finally, ensure that HTTP and HTTPS traffic is allowed if needed.

![Screenshot (2028)](https://github.com/user-attachments/assets/4a577a2f-f0b3-447c-baf2-4833fae2589c)

Now, SSH into your VM and run these commands:

```python
#Update package lists
sudo apt update

#Install Python 3, pip, and python3-venv
sudo apt install python3 python3-pip python3-venv

#Create a virtual environment in your home directory
python3 -m venv ~/airflow-env

#Activate the virtual environment
source ~/airflow-env/bin/activate

#Install pip within the virtual environment (if needed)
pip install --upgrade pip

#Install Apache Airflow
pip3 install apache-airflow

#Start Airflow in standalone mode
airflow standalone
```

Focus on these lines in the terminal:

```bash
standalone | Airflow is ready
standalone | Login with username: admin  password: airflow-password
```

To access your Airflow server, check your Firewall policies and create a new rule named "allow-airflow" if necessary, allowing TCP traffic on port 8080. Ensure the rule targets the network tags associated with your Airflow VM instances. Now, open the Airflow UI by going to `http://external-ip:8080`.

![Screenshot (2098)](https://github.com/user-attachments/assets/f531cde1-a0a4-4f04-bbca-54f3bebf206d)

After entering your username and password, you'll see a list of sample DAGs that are ready to use.

![Screenshot (2029)](https://github.com/user-attachments/assets/c316e067-e27a-4ba5-8df8-0b9724a6f2fd)

Remember, if you're working in a separate terminal, always activate your environment first: `source ~/airflow-env/bin/activate`.

Check the `dags_folder` path in your airflow.cfg file. In my case, it's `/home/jana/airflow/dags`. Create a directory named `dags` under your home directory (`~/airflow`). This is where you'll create your DAGs.

```bash
jana@airflow-vm: ls
airflow  airflow-env
jana@airflow-vm: cd airflow
jana@airflow-vm:~/airflow$ ls
airflow-webserver.pid  airflow.cfg  airflow.db  logs  standalone_admin_password.txt  webserver_config.py
jana@airflow-vm:~/airflow$ mkdir dags
jana@airflow-vm:~/airflow$ ls
airflow-webserver.pid  airflow.cfg  airflow.db  dags  logs  standalone_admin_password.txt  webserver_config.py
jana@airflow-vm:~/airflow$ cd dags/
jana@airflow-vm:~/airflow/dags$ pwd
/home/jana/airflow/dags
```

To remove DAG examples, locate the `load_examples = True` line in the airflow.cfg file and change it to `False`. Restart your VM to apply the changes.

Depending on the operators you plan to use in your DAG, you might need to install additional packages on your VM. If you're unsure about which packages are required, try uploading your DAG to the `dags` folder and checking the Airflow UI for any errors. For instance, a `ModuleNotFoundError: No module named 'airflow.providers.google'` indicates that your Airflow installation lacks the necessary packages  to interact with Google Cloud APIs. To resolve this, install the  required package using `pip install apache-airflow-providers-google` on your VM and restart Airflow. If you encounter any errors, consult the Airflow logs for more detailed information.

Any changes you make to your DAG files will be reflected immediately in the Airflow UI. If you encounter any syncing issues, stop the processes in your VM using `CTRL+C`, then restart them with the command `airflow standalone`.

Here are some essential Airflow commands:

**View Configuration**: `airflow config list`
**List DAGs**: `airflow dags list`
**Restart Webserver and Scheduler**: `airflow webserver -D`; `airflow scheduler -D`

Here is an example: 

```bash
$ airflow dags list
dag_id        | fileloc                            | owners  | is_paused
==============+==============================================+=========+
my_simple_dag | /home/jana/airflow/dags/example.py | airflow | True
```

For more CLI commands, check out the official Airflow [documentation](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#command-line-interface).


Unfortunately, each time you restart your VM, the External IP address will change. This means you'll need to manually update the URL link in your Cloud Run functions code. However, you can resolve this by assigning a Static External IP address. To do this, navigate to IP Addresses within your VPC Network and reserve a Static External IP address. Once you have a reserved static IP, you can assign it to your VM instance by editing the instance configuration. This will ensure your VM always has the same external IP address, providing stability for your applications and services.

![Screenshot (2099)](https://github.com/user-attachments/assets/ce36f13a-a140-4613-aa94-1962149208f2)


## To connect your Airflow to Bigquery

- Create a JSON key for either the Compute Engine default service account or a dedicated service account for your project. Upload the key to the VM.

- Create ''bigquery_default'' connection in Airflow UI. Add Connection Id, Connection Type, Project Id, Keyfile Path (path to the JSON key).


![1_WskoHvI0XuVYk4skRU7nBQ](https://github.com/user-attachments/assets/39e41400-9b5a-490d-86fd-3806cc646c71)

- Add or edit the OAuth Scopes for your VM. Essentially, your VM's Cloud API access scopes should be set to, e.g. to "Allow full access to all Cloud APIs." If you miss this step, you'll likely encounter an error: `google.api_core.exceptions.Forbidden: 403 Access Denied: BigQuery BigQuery: Missing required OAuth scope. Need BigQuery or Cloud Platform write scope.; reason: accessDenied, message: Access Denied: BigQuery BigQuery: Missing required OAuth scope. Need BigQuery or Cloud Platform write scope`.

![1_O56Uf-hlaHMs8RSzt21_RA](https://github.com/user-attachments/assets/f961947e-5b1d-45cd-ba52-f7039dcef04f)

---

Let's dive into the DAG. My data includes information on 1642 ports, but my primary focus is on a specific Lithuanian port: 'Klaipeda' (port ID ''port579''). This DAG, named ''gcs_to_bigquery_dag_test'', will handle the data pipeline from GCS to BigQuery. For simplicity, it will utilize WRITE_TRUNCATE for both data loading and filtering tasks, overwriting the destination tables with each run.

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

#Define your DAG
default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}
with DAG(
    dag_id='gcs_to_bigquery_dag_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    #Task to list objects in the GCS bucket with a specific prefix
    list_gcs_objects = GCSListObjectsOperator(
        task_id='list_gcs_objects',
        bucket='your-bucket',
        prefix='Daily_Port_Activity_Data_and_Trade_Estimates',
        delimiter='/',
        gcp_conn_id='google_cloud_default',
    )
    #Task to load the latest CSV file from GCS bucket to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='your-bucket',
        source_objects=list_gcs_objects.output,
        destination_project_dataset_table='project-id.data.daily_port_data',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )
    #Task to run a BigQuery query and insert the result into another table
    run_bq_query = BigQueryInsertJobOperator(
        task_id='run_bq_query',
        configuration={
            "query": {
                "query": """
                    SELECT *
                    FROM `project-id.data.daily_port_data`
                    WHERE portname IN ('Klaipeda')
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "project-id",
                    "datasetId": "data",
                    "tableId": "port579_data"
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        gcp_conn_id='bigquery_default',
        location='US',
    )
    #Define task dependencies
    list_gcs_objects >> load_to_bigquery >> run_bq_query
```

Tasks:

**list_gcs_objects**: Lists all objects within a specific GCS bucket, filtered by a given prefix. 

**load_to_bigquery**: Loads the CSV file, identified by the previous task, from GCS into a pre-existing BigQuery table, preserving all data for future analysis.

**run_bq_query**: Filters data for the 'Klaipeda' port from the newly loaded BigQuery table and inserts the results into another table, overwriting existing data.

You can already test your solution by uploading the CSV file to the bucket and triggering the DAG. You should be able to load the data to BigQuery.

![Screenshot (1994)](https://github.com/user-attachments/assets/7ddc5e89-c2a0-4900-88b8-7829b6fb7565)

# ⚡Cloud Run Functions

The cloud landscape is shifting rapidly! Just last week, Cloud Functions was the dominant choice for deploying event-driven applications, but now [Cloud Run functions](https://cloud.google.com/blog/products/serverless/google-cloud-functions-is-now-cloud-run-functions) is taking center stage. Let's explore this new service! This project was built on the first generation of Cloud Functions, so I've decided to keep the original code and see how it performs with the new service. According to [Google Cloud](https://cloud.google.com/functions/docs/concepts/version-comparison), they plan to continue supporting Cloud Functions (first generation), so you can rest assured your existing code will remain functional. Read about Cloud Run functions version comparison [here](https://cloud.google.com/functions/docs/concepts/version-comparison).

Cloud Run functions offer a powerful and seamless way to build and deploy serverless applications. You write single-purpose functions that respond to events like file uploads, changes in Cloud Storage, or messages on Pub/Sub, enabling you to build responsive and automated data pipelines. You can package your code into a container image and deploy it to Google's managed infrastructure with just a few commands. This streamlined process simplifies development and deployment, reducing time and effort. Cloud Run functions provide a flexible and scalable solution, allowing you to handle complex workloads and manage persistent state. With Google managing the underlying infrastructure, you can focus on building your applications with confidence.

![1_Zv5mBhLS3JRIITydPzdL2A](https://github.com/user-attachments/assets/0a2cda11-fcea-45d6-ace9-b7603d14d242)

First, let's talk about the Function for VMs. Navigate to Cloud Run functions and create one. Choose the correct trigger: GCS, on (finalizing/creating) file in the selected bucket. Pay attention to these parameters in the code: dag, regex, url, username, password. Set the entry point to ''trigger_dag''.

![Screenshot (2090)](https://github.com/user-attachments/assets/905b1a50-240c-4a08-b874-b11e8f37d56d)


```python
import requests
import re
import google.cloud.logging
import logging 

client = google.cloud.logging.Client()
client.setup_logging()

dag_trigger_rules = [
    { 'dag': 'gcs_to_bigquery_dag_test', 'regex': 'Daily_Port_Activity_Data_and_Trade_Estimates' }
]

def trigger_dag(data, context=None):

    object_name = data['name']
    logging.info('Triggering object Name: {}'.format(object_name))

    for rule in dag_trigger_rules:
        regex = rule['regex']
        if re.match(regex, object_name):
            dag_name = rule['dag']
            logging.info('Successfully triggered DAG: {}'.format(dag_name))
            return make_airflow_request(dag_name, data)
            break

def make_airflow_request(dag_name: str, data: dict) -> requests.Response:

    #Hardcoded username and password
    username = "admin"
    password = "airflow-password"

    #Airflow REST API endpoint for triggering DAG runs
    endpoint = f"api/v1/dags/{dag_name}/dagRuns"
    url = f"http://external-ip:8080/{endpoint}"  #Replace with your Airflow server URL and port

    #Create a session with basic authentication
    session = requests.Session()
    session.auth = (username, password)

    return session.post(url, json={"conf": data})
```

Add `requests` and `google-cloud-logging` to requirements.txt.

For clarity, the code uses hardcoded username and password credentials. However, in a real production setting, it's essential to protect these sensitive values. Instead of storing them directly in the code, you should leverage Google Cloud's Secret Manager to securely store and manage secrets. To learn more about using Secret Manager, refer to my previous article on [GCP Secret Manager](https://medium.com/apache-airflow/keep-your-airflow-variables-and-connections-safe-with-gcp-secret-manager-d2e9f68fbe4b).

Let's test and deploy our Cloud Run function. To enable [basic authentication](https://airflow.apache.org/docs/apache-airflow/2.5.0/security/api.html) for the Airflow API, we need to make a small configuration change. Open your airflow.cfg file and locate the `[api]` section. Within that section, modify the `auth_backends` line to read: `auth_backends = airflow.api.auth.backend.basic_auth`. This configuration allows your code to authenticate API requests using a  username and password. After making this change, restart your VM for  the configuration to take effect.

Now that your Airflow VM is running and your DAG is uploaded to the Airflow UI, you're ready to test your data pipeline. Drop your data file (in my case, "Daily_Port_Activity_Data_and_Trade_Estimates_2023.csv") to the bucket and check the logs of your Cloud Function. If everything is working correctly, you should see a message like "Successfully triggered DAG: gcs_to_bigquery_dag_test" in the logs. This indicates that the Function has successfully triggered your DAG, which will then load the CSV data into BigQuery.

![Screenshot (2109)](https://github.com/user-attachments/assets/84cabd2b-a7b5-4dfe-8cad-22c43d13e1e6)

This approach offers several advantages in terms of cost-effectiveness and overall efficiency:

**Pay-per-execution**: Google Cloud Run functions operate on a pay-per-execution model. You only pay for the resources consumed when the function is actually triggered. This means no ongoing costs for idle resources, making it very cost-effective for infrequent data loads.

**Serverless architecture**: Cloud Run functions are serverless, meaning you don't need to manage or provision any servers. This eliminates the overhead associated with server maintenance, scaling, and patching, further reducing costs.

We've seen how to trigger DAGs using Functions with a VM-based Airflow installation. But Composer offers a more integrated and managed solution, making it easier to work with Airflow on GCP. Let's dive in!

# ✨ Cloud Composer

As someone who spends a lot of time working with Composer, I have to say it's my favorite GCP service. While it can be pricey, the benefits are undeniable. Composer handles everything, from installing Airflow to managing a Kubernetes Autopilot cluster. This is especially valuable for large projects with many DAGs, as it eliminates the overhead of managing infrastructure and ensures a stable environment.

To get started, launch your Composer environment and upload your DAG file to the "DAGs" folder. No changes are needed to the DAG itself, and you don't have to install any additional packages. I tested this with both Composer 2 and Composer 3 (still in preview), and the logic and files remained the same for both versions. For Composer 2, I needed to activate my GCP account, while Composer 3 can still be created on a Free Trial.

![Screenshot (2064)](https://github.com/user-attachments/assets/0011fe0d-878d-4fc2-bb22-c73050e71b09)

The main change is in your Function code. Set the entry point to ''trigger_dag'' and retrieve the ''web_server_url'' from your Composer environment's configuration. This ''web_server_url'' replaces the previous ''url'' and handles authentication, so you won't need to provide any separate credentials like a ''username'' or ''password''. 

```python
import google.auth
from typing import Any
from google.auth.transport.requests import AuthorizedSession
import requests
import logging
import re
import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

dag_trigger_rules = [
    { 'dag': 'gcs_to_bigquery_dag_test', 'regex': 'Daily_Port_Activity_Data_and_Trade_Estimates' }
]

def trigger_dag(data, context=None):

    web_server_url = "https://composer-web-airflow-ui-dot-us-central1.composer.googleusercontent.com"

    object_name = data['name']
    logging.info('Triggering object Name: {}'.format(object_name))

    for rule in dag_trigger_rules:
        regex = rule['regex']
        if re.match(regex, object_name):
            dag_name = rule['dag']
            logging.info('Successfully triggered DAG: {}'.format(dag_name))
            endpoint = f"api/v1/dags/{dag_name}/dagRuns"
            url = f"{web_server_url}/{endpoint}"
            return make_composer2_web_server_request(url, method='POST', json={"conf": data})
            break

def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:

    authed_session = AuthorizedSession(CREDENTIALS)

    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)
```
Set entry point  to ''trigger_dag'', add `google-auth`, `requests`, `google-cloud-logging` to requirements.txt.


![Screenshot (2062)](https://github.com/user-attachments/assets/4a0aa0ce-68a5-4845-acf6-edd7efed5571)

Once everything is set up, you can access the Airflow UI to monitor and manage your DAGs. To test the solution, upload the files to the bucket.

![1_iJK7Su7sxE3zQY76E8llKA](https://github.com/user-attachments/assets/d05eff36-60a8-40c1-9d29-d48684db5ec7)

As you can see, the logs are the same as the VM Cloud Run function logs.

![Screenshot (2066)](https://github.com/user-attachments/assets/5df44a58-51ce-402d-9957-cec57e56576e)

# 🚨 Logging / Monitoring

GCP Logging and Monitoring services might not be the most exciting tools in the cloud toolbox, but they're absolutely crucial for keeping your applications healthy and your infrastructure running smoothly.

In this chapter, we'll create a Log Metric and an Alert based on Function logs. This is particularly useful for monitoring the Function's status and the overall data pipeline's health. Since our DAG isn't scheduled and is triggered by the Function only when files arrive in the bucket, we want to ensure everything is working as expected and receive notifications if data is late or missing. In real-world scenarios, with more complex architectures and multiple DAGs, you'd likely implement a more sophisticated notification system involving emails and alert tickets.

First, we'll create a Log-based Metric. Provide a name for the metric, define the filter based on the log entries you want to track (e.g., specific log messages related to DAG triggering). In this case, our filter will target the phrase "Successfully triggered DAG" since it's present in the Function code and visible in the logs. Pay attention to the Filter and resource type; while the service has changed to Cloud Run functions, the resource type may still be listed as "Cloud Function" when setting up your metric.

![Screenshot (2115)](https://github.com/user-attachments/assets/95ab059d-9476-4ec5-bf39-405b6b39ac9b)

Next, we'll set up an Alerting Policy. You can easily create an alert directly from the "Log-based Metric" by clicking "Create alert from metric" under the three-dot menu. When configuring the alert, pay close attention to the "Configure alert trigger" section. Here, you'll need to define the conditions for the alert. For example, you can select "Metric absence" as the "Condition Type" and set a "Trigger absence time" of 30 minutes. Additionally, you can specify a "Rolling window" of 10 minutes. This alert system will proactively detect potential issues with your data pipeline, such as failures in the Cloud Run function or problems with the DAG's execution.

Set up notification channels (like email or SMS) to receive alerts. I've added myself to the notification channel using my email address. This way, we'll be notified promptly if any issues arise that prevent the DAG from running, allowing us to quickly investigate and resolve them.

![Screenshot (2093)](https://github.com/user-attachments/assets/91e95251-a3f3-41fc-8b4e-1403a8fd6d1d)

Here's how the Function activity looks in the "Policy details" section. I tested my solution by dropping files into the bucket to initiate the pipeline.

![Screenshot (2077)](https://github.com/user-attachments/assets/1fec9956-7afa-46ee-b1d9-ad3a25a047ca)

You can also filter the specific logs in "Logs Explorer" or access this view directly from your created "Log-based Metric" by clicking on "View logs for metrics" under the three-dot menu.

![Screenshot (2067)](https://github.com/user-attachments/assets/977da964-7c20-43ae-b68f-b1e844e59f8c)

Depending on your alert configuration, specifically the "Trigger absence time" and "Rolling window" values, you'll see incidents in the dashboard if the log stream is absent. For more details, click on the "Incident details".

![Screenshot (2117)](https://github.com/user-attachments/assets/ec41e83c-c35c-47af-8602-26c65545db48)


![Screenshot (2081)](https://github.com/user-attachments/assets/7d0e4ccd-7f8d-48e6-a57e-9a89db7ef313)

As I configured the alerting policy to send alerts to my email, I received one. This demonstrates how configuring an alerting policy for your Cloud Run function logs helps you stay informed about the health of your data pipeline. You can then take prompt action to maintain data integrity.

![Screenshot (2079)](https://github.com/user-attachments/assets/4059dd47-8355-4349-ab75-be1dfbf52823)

Our data pipeline is now up and running, with CSV files triggering the Function, which in turn triggers the DAG that loads the data into two BigQuery tables. We've also implemented an alerting policy to ensure everything runs smoothly. Now, let's dive into BigQuery and discover the power of BigQuery ML for extracting valuable insights from our data.


# 🤖 BigQuery ML

I believe [BigQuery ML](https://cloud.google.com/bigquery/docs/bqml-introduction) deserves more attention, as it allows you to easily create and run ML models using Google SQL queries. Additionally, you can access Vertex AI models and Cloud AI APIs to perform advanced AI tasks like text generation and machine translation. Gemini for Google Cloud also adds AI-powered assistance for BigQuery tasks, making it even easier to work with large amounts of data. For more information on the AI-powered features of BigQuery, you can check out the [Gemini in BigQuery overview](https://cloud.google.com/gemini/docs/bigquery/overview).

Performing ML or AI on large datasets typically requires a high level of programming expertise and in-depth knowledge of ML frameworks, which limits the development of ML solutions to a small group of highly skilled individuals within an organization. BigQuery ML is a great entry point for data analysts who want to explore machine learning without needing deep programming expertise. Its SQL-based interface makes it accessible. It is a powerful tool for data engineers to build and deploy machine learning models as part of larger data pipelines and applications. Read about all the types of models that BigQuery ML supports [here](https://cloud.google.com/bigquery/docs/bqml-introduction#supported_models). BigQuery ML can also be useful for business analysts, data scientists, and anyone looking to leverage machine learning for data-driven insights. If you are completely new to ML, please check this amazing Machine Learning Glossary [here](https://developers.google.com/machine-learning/glossary/) which defines general machine learning terms. In this chapter I will provide a practical example on how to start using BigQuery ML.

---

Let's go back to the table with Lithuanian data in "port579_data". I will concentrate on one value as example: "portcalls".Let's go back to the table with Lithuanian data in "port579_data". I will concentrate on one value as example: "portcalls".

- **portcalls**: total number of ships entering the port at specific date. This is the sum of **portcalls_container**, **portcalls_dry_bulk**, **portcalls_general_cargo**, **portcalls_roro** and **portcalls_tanker**.

First, we will create a model. The following query creates a new model called "port579_portcalls_model" in the "sunny-emblem-423616-k6.data" dataset using linear regression. Linear regression is chosen for its simplicity. It is a straightforward model that assumes a linear relationship between the features and the target variable. It trains the model on the data from the "port579_data" table, filtering only the rows where the month is June and the year is between 2019 and 2023. `input_label_cols=['portcalls']` indicates that the ''portcalls'' column is the target variable to be predicted.

```SQL
CREATE OR REPLACE MODEL `sunny-emblem-423616-k6.data.port579_portcalls_model`
OPTIONS(model_type='linear_reg', input_label_cols=['portcalls']) AS
SELECT
  year,
  month,
  day,
  portid,
  portname,
  country,
  portcalls
FROM
  `sunny-emblem-423616-k6.data.port579_data`
WHERE
  month = 6 AND year >= 2019 AND year <= 2023
```

You will see your model under Models in the same dataset. Since the data is quite small, it takes few seconds to create the model. Pay attention to the schema of the model: Labels and Features.

![Screenshot (1981)](https://github.com/user-attachments/assets/2f7d7905-bbf9-4a61-82a2-88d06d04baf2)

Second, we will use the data for June 2024. I decided to go for June since I have only 7 rows in my data for July 2024. We'll skip using `ML.EVALUATE` here, as we've trained our model on the entire June 2019-2023 dataset.  Instead, we'll use `ML.PREDICT` to see how well our model performs on this new data.

The purpose of this code is to use the trained model "port579_portcalls_model" to predict the number of port calls for June 2024, based on the features in the data from that month.

```SQL
SELECT
  year,
  month,
  day,
  portid,
  portname,
  country,
  portcalls,
  predicted_portcalls
FROM
  ML.PREDICT(MODEL `sunny-emblem-423616-k6.data.port579_portcalls_model`,
    (
    SELECT
      year,
      month,
      day,
      portid,
      portname,
      country,
      portcalls
    FROM
      `sunny-emblem-423616-k6.data.port579_data`
    WHERE
      month = 6
      AND year = 2024 ) )
```

The result of the query will include the original input features along with the predicted values for the ''predicted_portcalls'' column, which are stored in the ''predicted_portcalls'' column. It's quite interesting to see the results. Here is an example.

![Screenshot (2026)](https://github.com/user-attachments/assets/fa6856c1-668c-4a54-9aa1-e0592a2939f7)

This result represents a specific data point for ''Klaipeda'' port in June 2024. The ''portcalls'' column contains the actual value of the port calls quantity. The ''predicted_portcalls'' column represents the predicted value for the same day and port, generated by the ''port579_portcalls_model''. As you can see only in some cases the predicted values are pretty accurate.

To see the whole picture let's create a visualization with 2 metrics, ''portcalls'' and ''predicted_portcalls'', in Looker Studio for better visibility.

![1_eDiUGKHtDTAwH3v954__TA](https://github.com/user-attachments/assets/f40c9cd8-6524-4bda-bd8d-e4bc28fdde54)

To understand the results better we need to check model's Evaluation section.

![Screenshot (1984)](https://github.com/user-attachments/assets/e1012a90-c8fd-48a5-9cfd-5474c61782c0)

The evaluation metrics suggest that the ''port579_portcalls_model''  is not performing very well. The MAE, MSE, and MSLE values are  relatively high, indicating that the model's predictions are not very  accurate and that our predictions are significantly different from the actual values. The R-squared value is low indicates that it's not capturing the relationships in our data effectively.  This means that the model is not able to  accurately predict the target variable based on the provided features. The lower value of MAE, MSE, and MSLE implies higher accuracy of a regression model. However, a higher value of R square is considered desirable.

**Can we make the model better?** Absolutely! To achieve this, we need to delve into Feature Engineering. We will experiment with different feature combinations to see if they improve the model's accuracy. This part is usually done by Data scientists. However, if you are interested, keep reading!

We will add and modify features to improve the model's accuracy. For this we need to investigate data further to understand the relationships between the features and the target variable. The low R-squared value and high error metrics (MAE, MSE, MSLE) indicate that the model is not explaining much of the variation in port calls. This is a key indicator of a problem. The right solution here would be to focus on the right features and metrics to ensure a better fit. Here are some ideas:

**Combine Related Features**: We already have ''portcalls'' calculated, but we also have individual ship types (container, dry bulk, etc.). Consider creating features like:

- **''total_portcalls_non_container''**: Sum of ''portcalls_dry_bulk'', ''portcalls_general_cargo'', ''portcalls_roro'', ''portcalls_tanker''.
  
- **''import_export_ratio''**: import / export (if you want to see if there's a relationship between import and export activity).

**Time-Based Features**: Create features that capture patterns over time:

- **''day_of_week''**: (Monday, Tuesday, etc.)

Let's run these queries to add new features, then create a new model, and finally make predictions with the enhanced data.

```sql
-- Add total_portcalls_non_container Column
ALTER TABLE `sunny-emblem-423616-k6.data.port579_data`
ADD COLUMN total_portcalls_non_container INT64;

-- Add import_export_ratio Column
ALTER TABLE `sunny-emblem-423616-k6.data.port579_data`
ADD COLUMN import_export_ratio FLOAT64; 

-- Add day_of_week Column
ALTER TABLE `sunny-emblem-423616-k6.data.port579_data`
ADD COLUMN day_of_week INT64; 

-- Create a New Model with New Features
CREATE OR REPLACE MODEL `sunny-emblem-423616-k6.data.port579_portcalls_model_new`
OPTIONS (model_type='linear_reg', input_label_cols=['portcalls']) AS
SELECT
    year,
    month,
    day,
    portid,
    portname,
    country,
    (portcalls_dry_bulk + portcalls_general_cargo + portcalls_roro + portcalls_tanker) AS total_portcalls_non_container,
    SAFE_DIVIDE(import, export) AS import_export_ratio,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    portcalls
  FROM
    `sunny-emblem-423616-k6.data.port579_data`
  WHERE month = 6
   AND year BETWEEN 2019 AND 2023
   AND NOT IS_NAN(SAFE_DIVIDE(import, export));

-- Calculate Features for June 2024
UPDATE
  `sunny-emblem-423616-k6.data.port579_data`
SET
  total_portcalls_non_container = ( portcalls_dry_bulk + portcalls_general_cargo + portcalls_roro + portcalls_tanker ),
  import_export_ratio = SAFE_DIVIDE(import, export),
  day_of_week = MOD(EXTRACT(DAYOFWEEK FROM date) + 5, 7) + 1  -- This expression adjusts the day OF the week TO match the standard ISO 8601 week numbering
WHERE
  month = 6
  AND year = 2024;

-- Make Predictions
SELECT
  *
FROM
  ML.PREDICT(MODEL `sunny-emblem-423616-k6.data.port579_portcalls_model_new`,
    (
    SELECT
      year,
      month,
      day,
      portid,
      portname,
      country,
      total_portcalls_non_container,
      import_export_ratio,
      day_of_week,
      portcalls
    FROM
      `sunny-emblem-423616-k6.data.port579_data`
    WHERE
      month = 6
      AND year = 2024 ) )
```

Let's see the results now.

![Screenshot (1987)](https://github.com/user-attachments/assets/b4a51efc-6f86-46b8-8f71-033f33e8c994)

Export results to the Looker Studio. We're seeing accurate lines in the Looker visualization because we've incorporated additional columns that provide more context and features to our model, resulting in more precise predictions.

![1_iPbjaL8K1aJlM15A_RdPeQ](https://github.com/user-attachments/assets/e15f211b-921b-48e4-b2bb-bc13fba4e351)

The Evaluation results are much better than before. The lower value of MAE, MSE, and MSLE implies higher accuracy of a regression model. The lower error metrics and the high R-squared value indicate that our model is now making more accurate predictions. Pay attention to R-squared changes. This is the most significant improvement! An R-squared of 0.814 means that our model explains about 81.4% of the variation in port calls. This is a substantial increase from the previous 6.86% and indicates that our model is now capturing the relationships in our data much more effectively. Adding features like ''total_portcalls_non_container'', ''import_export_ratio'', and ''day_of_week'' helped the model capture more complex relationships.

![Screenshot (1986)](https://github.com/user-attachments/assets/6da2418d-a620-4b2a-a4d1-16b162ad66c4)

By adding additional columns you can significantly improve your model. For example, ''day_of_week'' extracts the day of the week (1 for Monday, 7 for Sunday) from the ''date'' column. This information is valuable because port calls might exhibit patterns based on the day of the week. For example, certain ports might experience higher activity on weekdays compared to weekends. By including this feature, your model can learn the relationship between the day of the week and the number of port calls, leading to more accurate predictions for specific days. And ''import_export_ratio'' calculates the ratio of imports to exports, creating a new feature that captures the balance of trade activity at the port. This ratio might be a strong indicator of port activity. Ports with a higher import-to-export ratio could potentially have more port calls. Your model can learn this correlation and use it to improve predictions.

More columns mean more data points for your model to learn from. This helps the model identify complex relationships and patterns within the data. Each additional column represents a new feature that can contribute to the model's understanding of the target variable (port calls). By incorporating features that capture different aspects of the data, your model is likely to generalize better to new data and make more accurate predictions.

Feature engineering is a crucial part of the machine learning process. By thoughtfully adding relevant features to your data, you can often significantly improve the accuracy and generalizability of your models. It's a practice that requires careful consideration, but it can lead to much better results.

I hope I've convinced you to give BigQuery ML a try!😉 Here are a few reasons why you should consider it:

**Ease of Use**: It's designed to be accessible to users with a SQL background, simplifying the process of building and deploying machine learning models.

**Scalability and Performance**: Leverages the power and scale of BigQuery, allowing you to train models on massive datasets with high performance.

**Integration with Data**: Seamlessly integrates with your existing BigQuery data, eliminating the need for data movement or complex integrations.

**Cost-Effective**: Offers a pay-as-you-go pricing model, making it cost-effective for both small and large projects.

**Complete ML Workflow**: Provides a comprehensive platform for the entire ML workflow, from data preparation to model training, evaluation, and deployment.

---

I hope you found the data pipeline and BigQuery ML intro helpful! I've added more detailed instructions to my GitHub account. If you have any questions, feel free to reach out on [LinkedIn](https://www.linkedin.com/in/jana-polianskaja/).😊










