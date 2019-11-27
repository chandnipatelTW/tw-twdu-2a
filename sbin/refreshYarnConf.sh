#!/usr/bin/env bash

set -xe

echo "====Updating YARN Config on EMR===="

scp -r ./Conf/hadoop/* hadoop@emr-master.twdu-2a.training:/etc/hadoop/conf/

# To refresh YARN configuration, see commands from here:
# https://hadoop.apache.org/docs/r2.7.4/hadoop-yarn/hadoop-yarn-site/YarnCommands.html

ssh hadoop@emr-master.twdu-2a.training '
set -e
yarn rmadmin -refreshQueues
'
echo "====Refreshed YARN Config on EMR===="