#!/bin/bash

set -e

SPARK_HOME=~/work/spark

sbt package
${SPARK_HOME}/bin/spark-submit \
    --class org.scalaby.$1 \
    --master spark://kslisenko-pc:7077 \
    target/scala-2.10/scalaby-samples_2.10-1.0.jar

   
