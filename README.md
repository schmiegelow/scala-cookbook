scala-cookbook
==============

A collection of small recipes and scala utility classes, as well as java classes using traditional frameworks
to interact with scala.


Kafka load gen
==============

A shell script is provided in src/main/shell which runs a message generator with forex exchange 
rates in a topic called records. The sample documents are in src/test/resources

*set up kafka topic*

./bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic records --partitions 2 --replication-factor 1


*Check topic*

./bin/kafka-topics.sh --list --zookeeper localhost:2181

*display topic queue contents*

bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic records --from-beginning

