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
