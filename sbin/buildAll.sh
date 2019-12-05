#!/usr/bin/env bash
set -eo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Building Producer JARs===="
cd $DIR/../CitibikeApiProducer
./gradlew clean test bootJar

echo "====Building Consumer JARs===="
cd $DIR/../RawDataSaver
sbt test
sbt package

cd $DIR/../StationConsumer
sbt test
sbt package

cd $DIR/../StationTransformerNYC
sbt test
sbt package

cd $DIR/../Monitoring
sbt test
sbt package

