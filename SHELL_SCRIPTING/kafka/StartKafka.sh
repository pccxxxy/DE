#!/bin/bash

# Start Kafka



# Step 1: Start Zookeeper

kafka_2.12-2.8.0/bin/zookeeper-server-start.sh config/zookeeper.properties


# Step 2: Start Kafka server

## Open another terminal session and run
kafka_2.12-2.8.0/bin/kafka-server-start.sh config/server.properties


# Step 3: Create a topic named toll

## Open another terminal session and run
kafka_2.12-2.8.0/bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092