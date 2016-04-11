#!/bin/bash -e

rm -rf out.csv

hadoop jar /usr/hdp/2.4.0.0-169/hadoop-mapreduce/hadoop-streaming.jar -fs local -jt local -mapper mapper.py -reducer reducer.py -input payments.csv users.csv -output out.csv -file mapper.py -file reducer.py
