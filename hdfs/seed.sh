#!/bin/bash

set -e

function delete_hdfs_directory {
	directory=$1
  $hadoop_path fs -fs hdfs://$hdfs_server -test -d $directory
  if [ $? == 0 ]; then
      echo "Deleting "$directory
      $hadoop_path fs -fs hdfs://$hdfs_server -rm -r $directory
  fi
}

delete_hdfs_directory /free2wheelers/rawData/stationDataSF/checkpoints
delete_hdfs_directory /free2wheelers/rawData/stationDataMarseille/checkpoints
delete_hdfs_directory /free2wheelers/rawData/stationDataNYC/checkpoints
delete_hdfs_directory /free2wheelers/stationMart/checkpoints

$hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/rawData/stationDataSF/checkpoints \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/rawData/stationDataSF/data \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/rawData/stationDataMarseille/checkpoints \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/rawData/stationDataMarseille/data \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/rawData/stationDataNYC/checkpoints \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/rawData/stationDataNYC/data \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/stationMart/checkpoints \
&& $hadoop_path fs -fs hdfs://$hdfs_server -mkdir -p /free2wheelers/stationMart/data
