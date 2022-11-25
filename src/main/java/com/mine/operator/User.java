package com.mine.operator;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class User {
    public static void main(String[] args) throws Exception {
        Configuration conf  = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        conf.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 2000));

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/xiaoshaojian/Documents/Codes/Flink/FlinkLearn/input/user"));

//        EnvironmentSettings settings = EnvironmentSettings
//            .newInstance()
////            .useBlinkPlanner()
//            .inStreamingMode() // 使用流处理模式
//            .inBatchMode()
//            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration tableConf = tableEnv.getConfig().getConfiguration();
        tableConf.setString("table.exec.mini-batch.enabled", "true");
        // use 5 seconds to buffer input records
        tableConf.setString("table.exec.mini-batch.allow-latency", "5 s");
        // the maximum number of records can be buffered by each aggregate operator task
        tableConf.setString("table.exec.mini-batch.size", "200");
//        tableConf.setBoolean("table.exec.emit.early-fire.enabled" , true);
//        tableConf.setString("table.exec.emit.early-fire.delay","5 s");
        tableConf.setString("table.exec.state.ttl", "5 s");

        // 用户登录表
        tableEnv.executeSql(
            "CREATE TABLE user_login (" +
                "`pid` SMALLINT, " +
                "`gid` INT, " +
                "`p_mid` SMALLINT, " +
                "`mid` INT, " +
                "`uid` INT, " +
                "`ltype` TINYINT, " +
                "`reg_date` STRING, " +
                "`reg_time` INT, " +
                "`enter_date` STRING, " +
                "`enter_time` INT, " +
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
            "    COUNT(DISTINCT CASE WHEN ltype = 2 AND reg_date = login_date THEN uid ELSE 0 END) as account_nums, \n" +
            "    COUNT(DISTINCT CASE WHEN enter_date = login_date THEN uid ELSE 0 END) as register_nums, \n" +
            "    COUNT(DISTINCT uid) as dau, \n" +
            "    enter_date as register_date, \n" +
            "    login_date as report_date, \n" +
            "    window_start, \n" +
            "    window_end \n" +
//            "    TUMBLE_START(ts, INTERVAL '1' DAY) as window_start, \n" +
//            "    TUMBLE_END(ts, INTERVAL '1' DAY) as window_end \n" +
//            "FROM user_login\n" +
            "FROM TABLE(\n" +
            "    CUMULATE( TABLE user_login, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' DAY )\n" +
//            "    TUMBLE( TABLE user_login, DESCRIPTOR(ts), INTERVAL '1' DAY )\n" +
            ")" +
            "GROUP BY \n" +
//            "    TUMBLE(ts, INTERVAL '1' DAY), pid, gid, p_mid, mid, enter_date, login_date\n";
            "    pid, gid, p_mid, mid, enter_date, login_date, window_start, window_end\n";
        Table operatorData = tableEnv.sqlQuery(sql);
        tableEnv.toChangelogStream(operatorData).print("agg");

        tableEnv.executeSql(
            "CREATE TABLE operator (" +
                "`pid` SMALLINT, " +
                "`gid` INT, " +
                "`p_mid` SMALLINT, " +
                "`mid` INT, " +
                "`account_nums` BIGINT NOT NULL, " +
                "`register_nums` BIGINT NOT NULL, " +
                "`dau` BIGINT NOT NULL, " +
                "`register_date` STRING, " +
                "`report_date` STRING, " +
                "`window_start` TIMESTAMP(3) NOT NULL, " +
                "`window_end` TIMESTAMP(3) NOT NULL, " +
                "PRIMARY KEY (pid, gid, p_mid, mid, register_date, report_date) NOT ENFORCED" +
            ") WITH ( " +
                "'connector' = 'jdbc', " +
                "'driver' = 'com.mysql.jdbc.Driver', " +
                "'url' = 'jdbc:mysql://118.25.139.46:3306/gin?characterEncoding=utf8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai', " +
                "'username' = 'web_user', " +
                "'password' = '0vCARZLTeblA2tTMJ8si3TM8KOZv', " +
                "'table-name' = 'operation_day'" +
            ")"
        );

        String insertSql = "INSERT INTO operator (" + sql + ") ";

        System.out.println(insertSql);
        tableEnv.executeSql(insertSql).print();

        env.execute();
    }
}