#!/bin/sh

set -x

if [[ "$1" == "setup" ]]; then
   cd ..
   mvn clean package
   cd bin
   hadoop fs -rmr /projects/oozie/BlueprintsBulkload
   hadoop fs -mkdir /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../target/MRTitanLoader-1.0-SNAPSHOT-oozie/* /projects/oozie/BlueprintsBulkload
   shift
fi 

oozie job -config ../oozie/job.properties -run -verbose





