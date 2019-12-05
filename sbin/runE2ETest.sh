#!/usr/bin/env bash
set -eo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "====Running E2E Tests===="
cd $DIR/../E2ETests
sbt test



