package com.mine.sink;

import com.mine.datasource.Kafka;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class ToCk {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 2000));

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/xiaoshaojian/Documents/Codes/Flink/FlinkLearn/input/checkpoint"));

        SingleOutputStreamOperator<String> data = Kafka.getData(env);

        // Kafka 事务超时时间
        Properties properties = new Properties();
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10000);
        KafkaSink<String> firstSink = KafkaSink.<String>builder()
            .setBootstrapServers("106.55.198.234:9093,106.55.198.234:9094")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("firstSink")
                    .setKeySerializationSchema(new SimpleStringSchema())
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setKafkaProducerConfig(properties)
            .build();

//        data.sinkTo(firstSink);

        env.execute();
    }
}
