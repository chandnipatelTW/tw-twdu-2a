import datetime as dt
import time
import json
import boto3
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2017, 11, 21),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


def unique_stations(**context):
    print("*****", context['task_instance'].xcom_pull(task_ids='put_metric_to_cloudwatch'))
    print "Sample"


def all_stations_has_location(**context):
    print("*****", context['task_instance'].xcom_pull(task_ids='put_metric_to_cloudwatch'))
    print "Sample"


def push_metric_to_cloudwatch(**context):
    value = context['task_instance'].xcom_pull(task_ids='is_delivery_file_updated')
    cloudwatch = boto3.client('cloudwatch', region_name='ap-southeast-1')
    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'delivery_file',
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
    if value == 0:
        raise ValueError('Delivery file not updated')


def is_delivery_file_updated(**context):
    fileStatus = context['task_instance'].xcom_pull(task_ids='delivery_file_status')
    fileLastModified = json.loads(fileStatus)['FileStatus']['modificationTime']
    currentTime = int(round(time.time() * 1000))
    timeDifference = (currentTime - fileLastModified) / 1000 / 60
    return 1 if timeDifference > 5 else 0


def delivery_file_status(**context):
    url = "http://emr-master.twdu-2a.training:50070/webhdfs/v1/user/hadoop/kaleeaswari/words.txt?op=GETFILESTATUS"
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache"
    }
    response = requests.request("GET", url, headers=headers)
    return response.text


SPARK_TEST_STEPS = [
    {
        "Name": "calculate_pi",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"]},
    }
]

JOB_FLOW_OVERRIDES = {"Name": "PiCalc", "Steps": SPARK_TEST_STEPS,
                      "Instances": {"EmrManagedMasterSecurityGroup": "sg-0191086b8ab10f36f"}}

# MasterInstanceType, SlaveInstanceType, InstanceCount, InstanceGroups, InstanceFleets, Ec2KeyName, Placement, KeepJobFlowAliveWhenNoSteps,\
# TerminationProtected, HadoopVersion, Ec2SubnetId, Ec2SubnetIds, EmrManagedMasterSecurityGroup, EmrManagedSlaveSecurityGroup, \
# ServiceAccessSecurityGroup, AdditionalMasterSecurityGroups, AdditionalSlaveSecurityGroups

with DAG('TwoWheeler-Mart-Monitor',
         default_args=default_args,
         schedule_interval='5 * * * *',
         catchup=False
         ) as dag:
    delivery_file_status = PythonOperator(
        task_id='delivery_file_status',
        python_callable=delivery_file_status,
        provide_context=True)

    is_delivery_file_updated = PythonOperator(task_id='is_delivery_file_updated',
                                              python_callable=is_delivery_file_updated,
                                              provide_context=True)

    push_metric = PythonOperator(task_id='put_metric_to_cloudwatch',
                                 python_callable=push_metric_to_cloudwatch,
                                 provide_context=True)

    read_csv_cmd = """
        export AWS_DEFAULT_REGION=ap-southeast-1
        step=$(aws emr add-steps --cluster-id j-3FASPV1HTQSC2 --steps Type=Spark,Name="emr read words csv",ActionOnFailure=CONTINUE,Args=[--class,com.free2wheelers.apps.DeliveryFileReader,--jars,/tmp/free2wheelers-station-consumer_2.11-0.0.1.jar] | python -c 'import json,sys;obj=json.load(sys.stdin);print obj.get("StepIds")[0];')
        echo '========='$step
        aws emr wait step-complete --cluster-id j-3FASPV1HTQSC2 --step-id $step
    """

    unique_stations = BashOperator(task_id='unique_stations', bash_command=read_csv_cmd)

    delivery_file_status >> is_delivery_file_updated >> push_metric >> unique_stations

    all_stations_has_location = PythonOperator(task_id='all_stations_has_location',
                                           python_callable=all_stations_has_location, provide_context=True)

# push_metric >> unique_stations
# push_metric >> all_stations_has_location
