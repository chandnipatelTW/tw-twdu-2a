#!/usr/bin/env bash

set -xe

echo "====Updating SSH Config===="


echo "

Host emr-master.twdu-2a.training
    User hadoop

Host !github.com *
	User ec2-user
	IdentitiesOnly yes
	ForwardAgent yes
	DynamicForward 6789
    StrictHostKeyChecking no


Host *.twdu-2a.training !bastion.twdu-2a.training
    User ec2-user
    ForwardAgent yes
    ProxyCommand ssh bastion.twdu-2a.training -W %h:%p 2>/dev/null

Host bastion.twdu-2a.training
    User ec2-user
    HostName ec2-18-139-6-183.ap-southeast-1.compute.amazonaws.com
    DynamicForward 6789

" >> ~/.ssh/config

echo "====SSH Config Updated===="

echo "====Insert app config in zookeeper===="
scp ./zookeeper/seed.sh kafka-1.twdu-2a.training:/tmp/zookeeper-seed.sh
ssh kafka-1.twdu-2a.training '
set -e
export hdfs_server="emr-master.twdu-2a.training:8020"
export kafka_server="kafka-1.twdu-2a.training:9092"
export zk_command="zookeeper-shell kafka-1.twdu-2a.training:2181"
sh /tmp/zookeeper-seed.sh
'
echo "====Inserted app config in zookeeper===="

echo "====Insert yarn config in EMR===="
sh ./sbin/refreshYarnConf.sh
echo "====Inserted yarn config in EMR===="
#


echo "====Copy jar to ingester server===="
scp CitibikeApiProducer/build/libs/free2wheelers-citibike-apis-producer0.1.0.jar ingester.twdu-2a.training:/tmp/
echo "====Jar copied to ingester server===="

ssh ingester.twdu-2a.training '
set -e

function kill_process {
    query=$1
    pid=`ps aux | grep $query | grep -v "grep" |  awk "{print \\$2}"`

    if [ -z "$pid" ];
    then
        echo "no ${query} process running"
    else
        kill -9 $pid
    fi
}

station_nyc="station-nyc"
station_san_francisco="station-san-francisco"
station_marseille="station-marseille"

echo "====Kill running producers===="

kill_process ${station_nyc}
kill_process ${station_san_francisco}
kill_process ${station_marseille}

echo "====Runing Producers Killed===="

echo "====Deploy Producers===="

nohup java -jar /tmp/free2wheelers-citibike-apis-producer0.1.0.jar --spring.profiles.active=${station_nyc} --kafka.brokers=kafka-1.twdu-2a.training:9092 1>/tmp/${station_nyc}.log 2>/tmp/${station_nyc}.error.log &
nohup java -jar /tmp/free2wheelers-citibike-apis-producer0.1.0.jar --spring.profiles.active=${station_san_francisco} --producer.topic=station_data_sf --kafka.brokers=kafka-1.twdu-2a.training:9092 1>/tmp/${station_san_francisco}.log 2>/tmp/${station_san_francisco}.error.log &
nohup java -jar /tmp/free2wheelers-citibike-apis-producer0.1.0.jar --spring.profiles.active=${station_marseille} --kafka.brokers=kafka-1.twdu-2a.training:9092 1>/tmp/${station_marseille}.log 2>/tmp/${station_marseille}.error.log &

echo "====Producers Deployed===="
'


echo "====Configure HDFS paths===="
scp ./hdfs/seed.sh emr-master.twdu-2a.training:/tmp/hdfs-seed.sh

ssh emr-master.twdu-2a.training '
set -e
export hdfs_server="emr-master.twdu-2a.training:8020"
export hadoop_path="hadoop"
sh /tmp/hdfs-seed.sh
'

echo "====HDFS paths configured==="


echo "====Copy Raw Data Saver Jar to EMR===="
scp RawDataSaver/target/scala-2.11/free2wheelers-raw-data-saver_2.11-0.0.1.jar emr-master.twdu-2a.training:/tmp/
echo "====Raw Data Saver Jar Copied to EMR===="

scp sbin/go.sh emr-master.twdu-2a.training:/tmp/go.sh

ssh emr-master.twdu-2a.training '
set -e

source /tmp/go.sh

echo "====Kill Old Raw Data Saver===="

kill_application "StationDataNYCSaverApp"
kill_application "StationDataSFSaverApp"
kill_application "StationDataMarseilleSaverApp"

echo "====Old Raw Data Saver Killed===="

echo "====Deploy Raw Data Saver===="

nohup spark-submit --master yarn --queue streaming --deploy-mode cluster --class com.free2wheelers.apps.StationLocationApp --name StationDataNYCSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M /tmp/free2wheelers-raw-data-saver_2.11-0.0.1.jar kafka-1.twdu-2a.training:2181 "/free2wheelers/stationDataNYC" 1>/tmp/raw-station-data-nyc-saver.log 2>/tmp/raw-station-data-nyc-saver.error.log &

nohup spark-submit --master yarn --queue streaming --deploy-mode cluster --class com.free2wheelers.apps.StationLocationApp --name StationDataSFSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M /tmp/free2wheelers-raw-data-saver_2.11-0.0.1.jar kafka-1.twdu-2a.training:2181 "/free2wheelers/stationDataSF" 1>/tmp/raw-station-data-sf-saver.log 2>/tmp/raw-station-data-sf-saver.error.log &

nohup spark-submit --master yarn --queue streaming --deploy-mode cluster --class com.free2wheelers.apps.StationLocationApp --name StationDataMarseilleSaverApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --driver-memory 500M /tmp/free2wheelers-raw-data-saver_2.11-0.0.1.jar kafka-1.twdu-2a.training:2181 "/free2wheelers/stationDataMarseille" 1>/tmp/raw-station-data-marseille-saver.log 2>/tmp/raw-station-data-marseille-saver.error.log &

echo "====Raw Data Saver Deployed===="
'


echo "====Copy Station Consumers Jar to EMR===="
scp StationConsumer/target/scala-2.11/free2wheelers-station-consumer_2.11-0.0.1.jar emr-master.twdu-2a.training:/tmp/
scp StationConsumer/target/scala-2.11/classes/log4j.properties emr-master.twdu-2a.training:/tmp/

scp StationTransformerNYC/target/scala-2.11/free2wheelers-station-transformer-nyc_2.11-0.0.1.jar emr-master.twdu-2a.training:/tmp/
echo "====Station Consumers Jar Copied to EMR===="

scp sbin/go.sh emr-master.twdu-2a.training:/tmp/go.sh

ssh emr-master.twdu-2a.training '
set -e

source /tmp/go.sh


echo "====Kill Old Station Consumers===="

kill_application "StationApp"
kill_application "StationTransformerNYC"

echo "====Old Station Consumers Killed===="

echo "====Deploy Station Consumers===="

nohup spark-submit --master yarn --queue streaming --deploy-mode cluster --class com.free2wheelers.apps.StationApp --name StationApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0  --driver-memory 1000M --driver-java-options "-Dlog4j.configuration=file:/tmp/log4j.properties" \
                        --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/tmp/log4j.properties" \
                        --files "/tmp/log4j.properties" /tmp/free2wheelers-station-consumer_2.11-0.0.1.jar kafka-1.twdu-2a.training:2181 1>/tmp/station-consumer.log 2>/tmp/station-consumer.error.log &

nohup spark-submit --master yarn --queue streaming --deploy-mode cluster --class com.free2wheelers.apps.StationApp --name StationTransformerNYC --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0  --driver-memory 500M /tmp/free2wheelers-station-transformer-nyc_2.11-0.0.1.jar kafka-1.twdu-2a.training:2181 1>/tmp/station-transformer-nyc.log 2>/tmp/station-transformer-nyc.error.log &

echo "====Station Consumers Deployed===="
'

scp Monitoring/target/scala-2.11/free2wheelers-monitoring_2.11-0.0.1.jar emr-master.twdu-2a.training:/tmp/