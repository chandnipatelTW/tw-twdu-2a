import datetime as datetime
import time
import json
import boto3
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'me',
    'start_date': datetime.datetime(2017, 11, 21),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Is station mart updated in last 5 minutes?
def station_mart_last_modified_time(**context):
    url = "http://emr-master.twdu-2a.training:50070/webhdfs/v1/free2wheelers/stationMart/data/_SUCCESS?op=GETFILESTATUS"
    headers = {
        'content-type': "application/text",
        'cache-control': "no-cache"
    }
    response = requests.request("GET", url, headers=headers)
    last_modified_time = json.loads(response.text)['FileStatus']['modificationTime']
    print("Station Mart was last updated at", datetime.datetime.fromtimestamp(last_modified_time/1000))
    return last_modified_time

def has_station_mart_updated(**context):
    station_mart_last_update = context['task_instance'].xcom_pull(task_ids='station_mart_last_modified_time')
    now = int(round(time.time() * 1000))
    print("Station Mart HDFS directory was last updated at", station_mart_last_update)
    print("Now is", datetime.datetime.fromtimestamp(now/1000))
    minutes_diff = (now - station_mart_last_update) / 1000 / 60
    print("Minutes diff", minutes_diff)
    return 1 if minutes_diff < 5 else 0

def push_station_mart_updated_metric(**context):
    is_station_mart_updated = context['task_instance'].xcom_pull(task_ids='has_station_mart_updated')
    print("Pushing metrics to cloud watch")
    push_metric("has_station_mart_updated", is_station_mart_updated)
    if is_station_mart_updated == 0:
        raise ValueError('Station Mart has not updated in last 5 minutes!')


# Unique station Id validator
def run_station_id_validator():
    return """
        export AWS_DEFAULT_REGION=ap-southeast-1
        step=$(aws emr add-steps --cluster-id j-31NR7G6CYIGE8 --steps Type=Spark,Name="UniqueStationIdValidator",ActionOnFailure=CONTINUE,Args=[--master,yarn,--queue,monitoring,--deploy-mode,cluster,--driver-memory,500M,--conf,spark.executor.memory=2g,--conf,spark.cores.max=1,--class,com.free2wheelers.apps.UniqueStationIdValidator,/tmp/free2wheelers-monitoring_2.11-0.0.1.jar] | python -c 'import json,sys;obj=json.load(sys.stdin);print obj.get("StepIds")[0];')
        echo 'Unique StationId ValidatorValidator running...'$step
        aws emr wait step-complete --cluster-id j-31NR7G6CYIGE8 --step-id $step
        echo 'Unique StationId Validator completed!'
    """

def read_station_id_metric(**context):
    url = "http://emr-master.twdu-2a.training:50070/webhdfs/v1/free2wheelers/monitoring/UniqueStationIdValidator/output.txt?op=OPEN"

    headers = {'content-type': "application/json",'cache-control': "no-cache"}
    response = requests.request("GET", url, headers=headers)
    result = json.loads(response.text)
    print("Count of duplicate station ids is: ", result)
    return result

def push_station_id_metric(**context):
    count = context['task_instance'].xcom_pull(task_ids='read_station_id_metric')
    print("Pushing unique station Id metrics to cloud watch")
    push_metric("duplicate_station_id_count", count)


# Null latitude longitude validator
def run_lat_long_validator():
    return """
        export AWS_DEFAULT_REGION=ap-southeast-1
        step=$(aws emr add-steps --cluster-id j-31NR7G6CYIGE8 --steps Type=Spark,Name="LatitudeLongitudeValidator",ActionOnFailure=CONTINUE,Args=[--master,yarn,--queue,monitoring,--deploy-mode,cluster,--driver-memory,500M,--conf,spark.executor.memory=2g,--conf,spark.cores.max=1,--class,com.free2wheelers.apps.LatitudeLongitudeValidator,/tmp/free2wheelers-monitoring_2.11-0.0.1.jar] | python -c 'import json,sys;obj=json.load(sys.stdin);print obj.get("StepIds")[0];')
        echo 'Latitude Longitude Validator running...'$step
        aws emr wait step-complete --cluster-id j-31NR7G6CYIGE8 --step-id $step
        echo 'Latitude Longitude Validator completed!'
    """

def read_lat_long_metric(**context):
    url = "http://emr-master.twdu-2a.training:50070/webhdfs/v1/free2wheelers/monitoring/LatitudeLongitudeValidator/output.txt?op=OPEN"

    headers = {'content-type': "application/json",'cache-control': "no-cache"}
    response = requests.request("GET", url, headers=headers)
    result = json.loads(response.text)
    print("Count of null latitude or longitude is: ", result)
    return result

def push_lat_long_metric(**context):
    count = context['task_instance'].xcom_pull(task_ids='read_lat_long_metric')
    print("Pushing null latitude longitude metrics to cloud watch")
    push_metric("null_latitude_longitude_count", count)


def push_metric(name, value):
    cloudwatch = boto3.client('cloudwatch', region_name='ap-southeast-1')
    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': name,
                'Dimensions': [
                    {
                        'Name': 'source',
                        'Value': 'airflow'
                    },
                ],
                'Unit': 'None',
                'Value': value,
            },
        ],
        Namespace='TwoWheelers'
    )

with DAG('TwoWheeler-Mart-Monitor',
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
    push_station_mart_updated_metric = PythonOperator(task_id='push_station_mart_updated_metric',
                                 python_callable=push_station_mart_updated_metric,
                                 provide_context=True)


    run_station_id_validator = BashOperator(task_id='run_station_id_validator',
                                          bash_command=run_station_id_validator())
    read_station_id_metric = PythonOperator(task_id='read_station_id_metric',
                                          python_callable=read_station_id_metric,
                                          provide_context=True)
    push_station_id_metric = PythonOperator(task_id='push_station_id_metric',
                                          python_callable=push_station_id_metric,
                                          provide_context=True)


    run_lat_long_validator = BashOperator(task_id='run_lat_long_validator',
                                      bash_command=run_lat_long_validator())
    read_lat_long_metric = PythonOperator(task_id='read_lat_long_metric',
                                              python_callable=read_lat_long_metric,
                                              provide_context=True)
    push_lat_long_metric = PythonOperator(task_id='push_lat_long_metric',
                                 python_callable=push_lat_long_metric,
                                 provide_context=True)

    station_mart_last_modified_time >> has_station_mart_updated >> push_station_mart_updated_metric >> run_station_id_validator >>  read_station_id_metric >> push_station_id_metric >> run_lat_long_validator >> read_lat_long_metric >> push_lat_long_metric
