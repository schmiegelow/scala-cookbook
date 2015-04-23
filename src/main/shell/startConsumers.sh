#!/bin/sh
NUM_THREADS=$1
TOPIC=records
PARTITIONS=2
GROUP_ID=test0
ZK_HOSTS=localhost:2181

for i in $(seq 1 $NUM_THREADS); do
    java -cp scala-cookbook-0.0.1-SNAPSHOT-shaded.jar io.criticality.app.KafkaClientRunner $TOPIC $PARTITIONS $GROUP_ID$NUM_THREADS $ZKHOSTS \
    > logs/consumer/con.log.$NUM_THREADS 2> logs/err.log;
    done
    