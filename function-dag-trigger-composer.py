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

    web_server_url = "https://composer-airflow-web-ui-dot-us-central1.composer.googleusercontent.com"

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

    #Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)

#add google-auth, requests, google-cloud-logging to requirements.txt
#set the entry point to 'trigger_dag'
