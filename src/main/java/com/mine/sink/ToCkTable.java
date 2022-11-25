package com.mine.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ToCkTable {
    public static void main(String[] args) throws Exception {
        Configuration conf  = new Configuration();
//        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        //自定义端口
//        conf.setInteger(RestOptions.PORT, 8081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        conf.setString("table.exec.state.ttl", "5 s");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 2000));

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/xiaoshaojian/Documents/Codes/Flink/FlinkLearn/input/checkpoint"));

        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode() // 使用流处理模式
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 用户登录表
        tableEnv.executeSql(
            "CREATE TABLE user_login (" +
                "`pid` INT, " +
                "`gid` INT, " +
                "`p_mid` INT, " +
                "`mid` INT, " +
                "`uid` INT, " +
                "`reg_date` STRING, " +
                "`login_date` STRING, " +
                "`login_time` BIGINT, " +
//                "`ts` TIMESTAMP(3) METADATA FROM 'timestamp', " +
                "`ts` as TO_TIMESTAMP_LTZ(login_time, 3), " +
                "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
            ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'user-login'," +
                "'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "'properties.group.id' = 'my-group'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'" +
            ")"
        );

        String sql =
            "SELECT \n" +
            "    pid, \n" +
            "    gid, \n" +
            "    p_mid, \n" +
            "    mid, \n" +
            "    COUNT(DISTINCT uid) as dau, \n" +
            "    reg_date, \n" +
            "    login_date, \n" +
            "    window_start, \n" +
            "    window_end \n" +
            "FROM TABLE(\n" +
            "    CUMULATE( TABLE user_login, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' DAY )\n" +
            ")" +
            "GROUP BY \n" +
            "    pid, gid, p_mid, mid, reg_date, login_date, window_start, window_end\n";
        System.out.println(sql);
        Table operatorData = tableEnv.sqlQuery(sql);
        tableEnv.toDataStream(operatorData).print("agg");

        tableEnv.executeSql(
            "create table operator (" +
                "`pid` int, " +
                "`gid` int, " +
                "`p_mid` int, " +
                "`mid` int, " +
                "`dau` bigint, " +
                "`reg_date` string, " +
                "`report_date` string" +
            ") with ( " +
                "'connector' = 'kafka', " +
                "'topic' = 'operator', " +
                "'properties.bootstrap.servers' = '127.0.0.1:9092', " +
                "'format' = 'json'" +
            ")"
        );

        tableEnv.executeSql(
            "insert into operator select " +
                "pid, " +
                "gid, " +
                "p_mid, " +
                "mid, " +
                "dau, " +
                "reg_date, " +
                "login_date as report_date " +
            "from " + operatorData);

        env.execute();
    }
}
