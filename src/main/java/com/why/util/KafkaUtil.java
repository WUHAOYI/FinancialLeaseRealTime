package com.why.util;

import com.why.common.FinancialLeaseCommon;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by WHY on 2024/9/5.
 * Functions: 封装kafka相关的方法
 */
public class KafkaUtil {
    private static final Logger log = LoggerFactory.getLogger(KafkaUtil.class);

    /**
     * 创建Kafka消费者
     *
     * @param topicName
     * @param groupId
     * @param initializer
     * @return
     */
    public static KafkaSource<String> getKafkaConsumer(String topicName, String groupId, OffsetsInitializer initializer) {
        if (!KafkaTopicChecker.topicExists(FinancialLeaseCommon.KAFKA_BOOTSTRAP_SERVERS, topicName)) {
            // 处理 topic 不存在的情况，记录日志或抛出异常
            log.error("Kafka topic {} does not exist.", topicName);
            throw new RuntimeException("Kafka topic " + topicName + " does not exist.");
        }
        return KafkaSource.<String>builder()
                    .setBootstrapServers(FinancialLeaseCommon.KAFKA_BOOTSTRAP_SERVERS)
                    .setTopics(topicName)
                    .setGroupId(groupId)
                    .setStartingOffsets(initializer)
                    .setValueOnlyDeserializer(new DeserializationSchema<String>() { //设置Kafka消息中value的反序列化器
                        @Override
                        public String deserialize(byte[] message) throws IOException {
                            if (message != null && message.length != 0) {
                                return new String(message);
                            }
                            return null;
                        }

                        @Override
                        public boolean isEndOfStream(String nextElement) {
                            return false;
                        }

                        @Override
                        public TypeInformation<String> getProducedType() {
                            return BasicTypeInfo.STRING_TYPE_INFO;
                        }
                    })
                    .build();

    }
        /**
         * 创建Kafka生产者
         * @param topicName
         * @param transId
         * @return
         */
        public static KafkaSink<String> getKafkaProducer (String topicName, String transId)
        {
            Properties props = new Properties();
            props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, FinancialLeaseCommon.KAFKA_TRANSACTION_TIMEOUT);
            return KafkaSink.<String>builder()
                    .setBootstrapServers(FinancialLeaseCommon.KAFKA_BOOTSTRAP_SERVERS)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(topicName)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setKafkaProducerConfig(props)
                    .setTransactionalIdPrefix(transId)
                    .build();
        }
    }
