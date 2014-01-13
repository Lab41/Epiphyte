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

if [[ "$1" == "update" ]]; then
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/bulk-load.properties
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/bulk-load-id.properties
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/workflow.xml
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/systems.properties
   hadoop fs -put ../properties/bulk-load.properties /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../properties/bulk-load-id.properties /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../properties/systems.properties /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../oozie/workflow.xml /projects/oozie/BlueprintsBulkload
   shift
fi

if [[ "$1" == "update-jar" ]]; then
   cd ..
     mvn clean package
   cd bin

   hadoop fs -rm /projects/oozie/BlueprintsBulkload/bulk-load.properties
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/bulk-load-id.properties
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/workflow.xml
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/systems.properties
   hadoop fs -rm /projects/oozie/BlueprintsBulkload/lib/MRTitanLoader-*
   hadoop fs -put ../properties/bulk-load.properties /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../properties/bulk-load-id.properties /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../properties/systems.properties /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../oozie/workflow.xml /projects/oozie/BlueprintsBulkload
   hadoop fs -put ../target/MRTitanLoader-1.0*.jar /projects/oozie/BlueprintsBulkload/lib
   shift
fi

if [[ "$1" == "noid" ]]; then
    oozie job -config ../oozie/job.properties -run -verbose
fi

if [[ "$1" == "id" ]]; then
    oozie job -config ../oozie/job-id.properties -run -verbose
fi





