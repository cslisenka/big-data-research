#!/bin/bash
cd ../spark
# master
./sbin/start-master.sh
# worker 1
./bin/spark-class org.apache.spark.deploy.worker.Worker spark://kslisenko-pc:7077 & 
# worker 2
./bin/spark-class org.apache.spark.deploy.worker.Worker spark://kslisenko-pc:7077 & 
