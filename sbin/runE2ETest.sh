#!/usr/bin/env bash
set -eo pipefail

echo "====Updating SSH Config===="

echo "

Host emr-master.twdu-2a-qa.training
    User hadoop

Host !github.com *
    User ec2-user
	  IdentitiesOnly yes
	  ForwardAgent yes
	  DynamicForward 6789
        StrictHostKeyChecking no


Host *.twdu-2a-qa.training !bastion.twdu-2a-qa.training
    User ec2-user
    ForwardAgent yes
    ProxyCommand ssh bastion.twdu-2a-qa.training -W %h:%p 2>/dev/null

Host bastion.twdu-2a-qa.training
    User ec2-user
    HostName ec2-52-221-9-233.ap-southeast-1.compute.amazonaws.com
    DynamicForward 6789

" >> ~/.ssh/config

echo "====SSH Config Updated===="


echo "====Running E2E Tests on EMR===="
ssh emr-master.twdu-2a-qa.training '
set -e
cd /tmp/E2ETests
sbt test
'
echo "==== Test ends ==="





