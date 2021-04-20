#!/usr/bin/env bash

rm *.class
rm *.jar
rm output.txt
hadoop com.sun.tools.javac.Main ArrayListWritable.java PageRank.java
jar cvf pagerank.jar *.class
hadoop fs -rm -r input
hadoop fs -rm -r output
hadoop fs -mkdir input
hadoop fs -put input/* input
hadoop jar pagerank.jar PageRank input output
hadoop fs -cat output/* | tee output.txt
