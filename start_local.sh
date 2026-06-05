#!/bin/bash
echo "Starting Zookeeper..."
/workspaces/kafka/bin/zookeeper-server-start.sh /workspaces/kafka/config/zookeeper.properties &
sleep 5

echo "Starting Kafka..."
/workspaces/kafka/bin/kafka-server-start.sh /workspaces/kafka/config/server.properties &
sleep 5

echo "Creating topics..."
/workspaces/kafka/bin/kafka-topics.sh --create \
    --topic clickstream \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo "Done. Kafka running on localhost:9092"
