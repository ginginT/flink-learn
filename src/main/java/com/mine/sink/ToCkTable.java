package com.mine.sink;

import com.mine.datasource.Kafka;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class ToCkTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 2000));

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/xiaoshaojian/Documents/Codes/Flink/FlinkLearn/input/checkpoint"));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String tableName = "user_login";

        tableEnv.executeSql(
            "CREATE TABLE " + tableName + " (" +
                "`pid` int, " +
                "`gid` int, " +
                "`p_mid` int, " +
                "`mid` int, " +
                "`uid` int, " +
                "`login_date` string, " +
                "`dateqq` string, " +
                "`ts` TIMESTAMP(3) METADATA FROM 'timestamp', " +
                "watermark for ts as ts - interval '10' second" +
            ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'first'," +
                "'properties.bootstrap.servers' = '106.55.198.234:9093,106.55.198.234:9094'," +
                "'properties.group.id' = 'my-group'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'" +
            ")"
        );

        Table userLoginRes = tableEnv.sqlQuery(
            "select " +
                "pid, " +
                "count(distinct uid) as dau, " +
                "gid, " +
                "p_mid, " +
                "mid, " +
                "login_date, " +
                "window_start, " +
                "window_end " +
            "from table(" +
                "cumulate( table " + tableName + ", descriptor(ts), interval '5' second, interval '1' day )" +
            ")" +
            "group by " +
                "pid, gid, p_mid, mid, login_date, window_start, window_end"
        );

        tableEnv.toDataStream(userLoginRes).print("userLoginRes");

        tableEnv.executeSql(
            "create table output_user_login (" +
                "`pid` int, " +
                "`gid` int, " +
                "`p_mid` int, " +
                "`mid` int, " +
                "`dau` bigint, " +
                "`login_date` string" +
            ") with ( " +
                "'connector' = 'kafka', " +
                "'topic' = 'first-result', " +
                "'properties.bootstrap.servers' = '106.55.198.234:9093,106.55.198.234:9094', " +
                "'format' = 'json', " +
                "'sink.partitioner'= 'round-robin'" +
            ")"
        );

        tableEnv.executeSql(
            "insert into output_user_login select " +
                "pid, " +
                "gid, " +
                "p_mid, " +
                "mid, " +
                "dau, " +
                "login_date " +
            "from " + userLoginRes);

        // Kafka 事务超时时间
//        Properties properties = new Properties();
//        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10000);
//        KafkaSink<String> firstSink = KafkaSink.<String>builder()
//            .setBootstrapServers("106.55.198.234:9093,106.55.198.234:9094")
//            .setRecordSerializer(
//                KafkaRecordSerializationSchema.builder()
//                    .setTopic("firstSink")
//                    .setKeySerializationSchema(new SimpleStringSchema())
//                    .setValueSerializationSchema(new SimpleStringSchema())
//                    .build()
//            )
//            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//            .setKafkaProducerConfig(properties)
//            .build();

//        data.sinkTo(firstSink);

        env.execute();
    }
}
