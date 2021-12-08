
import datetime
import os
from airflow.operators import email
from airflow import models
from airflow.utils import trigger_rule
from airflow.providers.google.cloud.operators import bigquery
from airflow.contrib.operators import dataproc_operator
import json
from io import StringIO
from google.cloud import storage
from datetime import timedelta
from google.cloud import bigquery
import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
import datetime
import requests
from airflow.operators.python import BranchPythonOperator
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import requests
import pandas as pd

bucket = "testcomtofun"
log_table_name = "grand-presence-325905.movies_metadata.log_table"
fact_id = "grand-presence-325905.movies_metadata.Fact_table"
stage_id = "grand-presence-325905.movies_metadata.Staging_table"

def check_fact_table():
    try:

        client = bigquery.Client()
        client.get_table(fact_id)
        return "fetch_filter"
        
    except NotFound:
        return "full_load_api_call"

def invoke_cloud_function():

    params = {"message":"tab1","api":"http://3f79-34-73-6-200.ngrok.io"}#Parameters if required
    #url = "https://us-central1-modular-temple-328004.cloudfunctions.net/full_load" 
    url = "https://us-central1-modular-temple-328004.cloudfunctions.net/secret_manager"#Your CF URL
    request = google.auth.transport.requests.Request()  #this is a request for obtaining the the credentials
    id_token_credentials = id_token_credential_utils.get_default_id_token_credentials(url, request=request)
    #resp = AuthorizedSession(id_token_credentials).request("GET", url=url,params=params) # the authorized session object is used to access the Cloud Function
    resp = AuthorizedSession(id_token_credentials).request("GET", url=url)
    #ti.xcom_push(key='response', value=resp)
    return resp.text



def check_table(ti):
    
    try:
        bucket_name = 'testcomtofun' # Replace it with your own bucket name.
        #data_path = 'sample_table1/2021-09-23 06:30:40.798905/Json_object.json'
        data_path = ti.xcom_pull(task_ids='delta_load_api_call',key='return_value')
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        data_blob = bucket.blob(data_path)
        data_str = data_blob.download_as_text()
        data_dowloaded_df = pd.read_json(StringIO(data_str))
        #pd_df = pd.read_json("gs://{bucket}/{table_name}".format(bucket=bucket,table_name='"'+table_name+'"'))
        return "partial_load_job"
    except ValueError:
        return "empty_table_notification"
    

def fetch_filter(ti):

    table_name = ti.xcom_pull(key='return_value', task_ids='full_load_api_call')
    query = """
            SELECT ModifiedDT from {log_table} where Table_name = {table_name}

        """.format(log_table = log_table_name, table_name = '"'+table_name+'"')
    client = bigquery.Client()

    df= (
        client.query(query)
        .result()
        .to_dataframe(
        # Optionally, explicitly request to use the BigQuery Storage API. As of
        # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
        # API is used by default.
            create_bqstorage_client=True,
        )
    )

    a = df["ModifiedDT"].values[0]
    ti.xcom_push(key='filter', value=a)

def get_timestamp(ti):
    table_name = ti.xcom_pull(key='return_value', task_ids='delta_load_api_call')
    ts = table_name
    ts = ts.split('/')[1]
    ti.xcom_push(key='timestamp', value=ts)
    ti.xcom_push(key='table_name', value=table_name)        

def get_timestamp1(ti):
    table_name = ti.xcom_pull(key='return_value', task_ids='full_load_api_call')
    ts = table_name
    ts = ts.split('/')[1]
    ti.xcom_push(key='timestamp1', value=ts)
    ti.xcom_push(key='table_name1', value=table_name)  


yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
default_dag_args = {
    'depends_on_past': False,
    'start_date': yesterday,
    'email': models.Variable.get('email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}



with models.DAG(
        'Dag_template',
        schedule_interval=datetime.timedelta(weeks=4),
        default_args=default_dag_args) as dag:
    
    check = BranchPythonOperator(task_id="check_fact_table", python_callable=check_fact_table)

    check1 = BranchPythonOperator(task_id="check_table_data", python_callable=check_table)


    fetch_filter = python_operator.PythonOperator(
        task_id='fetch_filter',
        python_callable=fetch_filter)
    
    #for unauth cloud functions

    simple_http = SimpleHttpOperator(
        task_id= "delta_load_api_call",
        method='GET',
        http_conn_id='my_gcf_conn',
        endpoint='delta_load',
        headers={},
        data = {"api":models.Variable.get('api'),"message":"tab1","filter":"2021-07-01 02:22:12"}
    )

    simple_http1 = SimpleHttpOperator(
        task_id= "full_load_api_call",
        method='GET',
        http_conn_id='my_gcf_conn',
        endpoint='function2',
        headers={},
        data = {"api":models.Variable.get('api'),"message":"tab1"}
    )

    # for authorized cloud functions

    http1 = python_operator.PythonOperator(task_id="Full_Load", python_callable=invoke_cloud_function) 




    get_current_t = python_operator.PythonOperator(

        task_id='get_current_timestamp',
        python_callable=get_timestamp,

        ) 
    get_current_t1 = python_operator.PythonOperator(

        task_id='get_current_timestamp1',
        python_callable=get_timestamp1,

        )

    dataproc_job = dataproc_operator.DataprocWorkflowTemplateInstantiateOperator(
        # The task id of your job
        task_id="partial_load_job",
        # The template id of your workflow
        template_id="partial_load",
        project_id='grand-presence-325905',
        region="us-central1",
        parameters={"TABLE_NAME": "{{ ti.xcom_pull(task_ids='delta_load_api_call',key='return_value') }}","CURRENT_TIME":"2021-07-01 02:22:12"}
    )

    dataproc_job1 = dataproc_operator.DataprocWorkflowTemplateInstantiateOperator(
        # The task id of your job
        task_id="full_load_job",
        # The template id of your workflow
        template_id="full_load",
        project_id='grand-presence-325905',
        region="us-central1",
        parameters={"TABLE_NAME": "{{ ti.xcom_pull(task_ids='full_load_api_call',key='return_value') }}","CURRENT_TIME":"2021-07-01 02:22:12"}
    )

    empty_table_notification = email.EmailOperator(
        task_id='empty_table_notification',
        to='snehilsingh0005@gmail.com',
        subject='No Incremental Data Received',
        html_content="""
        No Incremental Data Received, Skipped Data Loading Tasks.
        """
        
        )

    full_load_success = email.EmailOperator(
        task_id='full_load_success',
        to='snehilsingh0005@gmail.com',
        subject='Full Load Dataproc Job Completed',
        html_content="""
        Full Load Job completed and tables are now available in BQ
        """
        
        )

    delta_load_success = email.EmailOperator(
        task_id='delta_load_success',
        to='snehilsingh0005@gmail.com',
        subject='Delta Load Dataproc Job Completed',
        html_content="""
        Delta Load Job completed and tables are updated and now available in BQ
        """
        
        )




    (check>>simple_http1>>get_current_t1>>dataproc_job1>>full_load_success)
    (check>>fetch_filter>>simple_http>>get_current_t>>check1>>dataproc_job>>delta_load_success)
    (check1>>empty_table_notification)


'''
Pipeline run ID 
pipeline run date time 
full data or incremental data 
number of records added: number of records updated or number of records added
'''

