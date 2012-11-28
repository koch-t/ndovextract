#!/bin/bash
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

./create-gtfs-gvb.sh
./create-gtfs-arriva.sh
./create-gtfs-qbuzz.sh
./create-gtfs-veolia.sh
./create-gtfs-ebs.sh
./create-gtfs-htm.sh
./create-gtfs-syntus.sh
