import datetime as dt
import time
import json
import boto3
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2017, 11, 21),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

def station_mart_last_modified_time(**context):
    url = "http://emr-master.twdu-2a.training:50070/webhdfs/v1/free2wheelers/stationMart/data/_SUCCESS?op=GETFILESTATUS"
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
    }
    response = requests.request("GET", url, headers=headers)
    last_modified_time = json.loads(response.text)['FileStatus']['modificationTime']
    print("Station Mart was last updated at", datetime.fromtimestamp(last_modified_time/1000))
    return last_modified_time


def has_station_mart_updated(**context):
    station_mart_last_update = context['task_instance'].xcom_pull(task_ids='station_mart_last_modified_time')
    now = int(round(time.time() * 1000))
    print("Station Mart HDFS directory was last updated at", station_mart_last_update)
    print("Now is", datetime.fromtimestamp(now/1000))
    minutes_diff = (now - station_mart_last_update) / 1000 / 60
    print("Minutes diff", minutes_diff)
    return 1 if minutes_diff < 5 else 0


def push_metric_to_cloud_watch(**context):
    is_station_mart_updated = context['task_instance'].xcom_pull(task_ids='has_station_mart_updated')
    if is_station_mart_updated == 0:
        raise ValueError('Station Mart has not updated in last 5 minutes!')
    print("Pushing metrics to cloud watch")
    cloudwatch = boto3.client('cloudwatch', region_name='ap-southeast-1')
    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'station_mart_updated',
                'Dimensions': [
                    {
                        'Name': 'source',
                        'Value': 'airflow'
                    },
                ],
                'Unit': 'None',
                'Value': is_station_mart_updated,
            },
        ],
        Namespace='TwoWheelers'
    )


with DAG('TwoWheeler-Mart-Monitor-Test-a',
         default_args=default_args,
         schedule_interval='5 * * * *',
         catchup=False
         ) as dag:
    station_mart_last_modified_time = PythonOperator(
        task_id='station_mart_last_modified_time',
        python_callable=station_mart_last_modified_time,
        provide_context=True)

    has_station_mart_updated = PythonOperator(task_id='has_station_mart_updated',
                                              python_callable=has_station_mart_updated,
                                              provide_context=True)

    push_metric = PythonOperator(task_id='push_metric_to_cloud_watch',
                                 python_callable=push_metric_to_cloud_watch,
                                 provide_context=True)

    station_mart_last_modified_time >> has_station_mart_updated >> push_metric
