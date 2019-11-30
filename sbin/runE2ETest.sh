#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Building Producer JARs===="
$DIR/../CitibikeApiProducer/gradlew -p $DIR/../CitibikeApiProducer clean bootJar
echo "====Building Consumer JARs===="
cd $DIR/../StationConsumer && sbt package
echo "====Running docker-compose===="

docker-compose -f $DIR/../docker/docker-compose-test.yml up --build -d

docker-compose -f $DIR/../docker/docker-compose-test.yml exec e2e-test sbt test

RESULT=$?

docker-compose -f $DIR/../docker/docker-compose-test.yml down

exit "$RESULT"
