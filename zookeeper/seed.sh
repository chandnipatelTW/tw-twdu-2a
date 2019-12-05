#!/bin/sh
echo $zk_command
$zk_command rmr /free2wheelers
$zk_command create /free2wheelers ''

$zk_command create /free2wheelers/stationDataNYC ''
$zk_command create /free2wheelers/stationDataNYC/kafkaBrokers $kafka_server
$zk_command create /free2wheelers/stationDataNYC/topic station_data_nyc
$zk_command create /free2wheelers/stationDataNYC/checkpointLocation hdfs://$hdfs_server/free2wheelers/rawData/stationDataNYC/checkpoints
$zk_command create /free2wheelers/stationDataNYC/dataLocation hdfs://$hdfs_server/free2wheelers/rawData/stationDataNYC/data

$zk_command create /free2wheelers/stationDataSF ''
$zk_command create /free2wheelers/stationDataSF/kafkaBrokers $kafka_server
$zk_command create /free2wheelers/stationDataSF/topic station_data_sf
$zk_command create /free2wheelers/stationDataSF/checkpointLocation hdfs://$hdfs_server/free2wheelers/rawData/stationDataSF/checkpoints
$zk_command create /free2wheelers/stationDataSF/dataLocation hdfs://$hdfs_server/free2wheelers/rawData/stationDataSF/data


$zk_command create /free2wheelers/stationDataMarseille ''
$zk_command create /free2wheelers/stationDataMarseille/kafkaBrokers $kafka_server
$zk_command create /free2wheelers/stationDataMarseille/topic station_data_marseille
$zk_command create /free2wheelers/stationDataMarseille/checkpointLocation hdfs://$hdfs_server/free2wheelers/rawData/stationDataMarseille/checkpoints
$zk_command create /free2wheelers/stationDataMarseille/dataLocation hdfs://$hdfs_server/free2wheelers/rawData/stationDataMarseille/data


$zk_command create /free2wheelers/output ''
$zk_command create /free2wheelers/output/checkpointLocation hdfs://$hdfs_server/free2wheelers/stationMart/checkpoints
$zk_command create /free2wheelers/output/dataLocation hdfs://$hdfs_server/free2wheelers/stationMart/data
