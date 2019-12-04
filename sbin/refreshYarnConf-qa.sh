#!/usr/bin/env bash

set -xe

echo "====Updating YARN Config on EMR===="

scp -r ./Conf/hadoop/capacity-scheduler.xml hadoop@emr-master.twdu-2a-qa.training:/etc/hadoop/conf/capacity-scheduler.xml
scp -r ./Conf/hadoop/yarn-site-qa.xml hadoop@emr-master.twdu-2a-qa.training:/etc/hadoop/conf/yarn-site.xml

# To refresh YARN configuration, see commands from here:
# https://hadoop.apache.org/docs/r2.7.4/hadoop-yarn/hadoop-yarn-site/YarnCommands.html

ssh hadoop@emr-master.twdu-2a-qa.training '
set -e
yarn rmadmin -refreshQueues
'
echo "====Refreshed YARN Config on EMR===="