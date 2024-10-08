#!/bin/bash

# Kafka broker 地址和端口
KAFKA_BROKER="hadoop108:9092"

# 获取所有 Kafka topics
topics=$(kafka-topics.sh --bootstrap-server $KAFKA_BROKER --list)

# 遍历每个 topic，找到以 financial_dwd_ 开头的，并删除它
for topic in $topics; do
    if [[ $topic == financial_dwd_* ]]; then
        echo "Deleting topic: $topic"
        kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic $topic
    fi
done
