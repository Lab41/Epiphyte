#!/bin/sh
set -x
set -e

if [[ $# != 1 ]]; then
    echo "Usage: Driver.sh <[file|hdfs]:\\properties_file>"
    exit 1
fi

if [[ "$DEBUG_ENABLED" -eq 1 ]]; then
	export HADOOP_OPTS="$HADOOP_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
	hadoop jar ../target/MRTitanLoader-1.0-SNAPSHOT.jar org.lab41.mapreduce.BlueprintsGraphDriver $@
else
	hadoop jar ../target/MRTitanLoader-1.0-SNAPSHOT.jar org.lab41.mapreduce.BlueprintsGraphDriver $@
fi
