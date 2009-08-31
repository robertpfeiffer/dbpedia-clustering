#!/bin/bash
cd $1
hadoop fs -copyFromLocal subjects* subjects*
hadoop fs -copyFromLocal centers* centers*
hadoop fs -copyFromLocal config.xml config.xml

(time hadoop jar ~/clustering.jar k-means centers* subjects* out > log.txt 2> err.txt) 2> time.txt 

hadoop fs -copyToLocal out out
hadoop fs -rmr out
hadoop fs -rm subjects*
hadoop fs -rm config.xml
