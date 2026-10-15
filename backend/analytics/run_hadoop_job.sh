#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: run_hadoop_job.sh <hdfs-input-file> <hdfs-output-directory>" >&2
  exit 2
fi

input_path="$1"
output_path="$2"
streaming_jar="$(find "${HADOOP_HOME:?HADOOP_HOME is not configured}/share/hadoop/tools/lib" \
  -name 'hadoop-streaming-*.jar' -print -quit)"

if [ -z "$streaming_jar" ]; then
  echo "The Hadoop Streaming JAR was not found." >&2
  exit 1
fi

hadoop fs -test -e "$input_path"
hadoop jar "$streaming_jar" \
  -D mapreduce.job.name=hospital-analytics \
  -D stream.map.input.ignoreKey=true \
  -files /opt/hospital-analytics/mapper.pl,/opt/hospital-analytics/reducer.pl \
  -input "$input_path" \
  -output "$output_path" \
  -mapper "perl mapper.pl" \
  -reducer "perl reducer.pl" \
  -numReduceTasks 1
