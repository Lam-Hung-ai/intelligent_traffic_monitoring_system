#!/bin/bash
set -e

echo "socket.request.max.bytes=30485760" >> /opt/kafka/config/server.properties
echo "message.max.bytes=10485760" >> /opt/kafka/config/server.properties
echo "replica.fetch.max.bytes=10485760" >> /opt/kafka/config/server.properties
echo "compression.type=zstd" >> /opt/kafka/config/server.properties

KAFKA_CLUSTER_ID="$(/opt/kafka/bin/kafka-storage.sh random-uuid)"
/opt/kafka/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c /opt/kafka/config/server.properties