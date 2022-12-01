package com.mine.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ToCkTableJoin1111 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 2000));

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///Users/xiaoshaojian/Documents/Codes/Flink/FlinkLearn/input/checkpoint"));

//        EnvironmentSettings settings = EnvironmentSettings
//            .newInstance()
//            .inStreamingMode() // 使用流处理模式
//            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration tableConf = tableEnv.getConfig().getConfiguration();
        tableConf.setString("table.local-time-zone", "Asia/Shanghai");
        tableConf.setString("table.exec.mini-batch.enabled", "true");
        // use 5 seconds to buffer input records
        tableConf.setString("table.exec.mini-batch.allow-latency", "5 s");
        // the maximum number of records can be buffered by each aggregate operator task
        tableConf.setString("table.exec.mini-batch.size", "200");
        tableConf.setBoolean("table.exec.emit.early-fire.enabled" , true);
        tableConf.setString("table.exec.emit.early-fire.delay","5 s");
        tableConf.setString("table.exec.state.ttl", "1 DAY");

        // 用户登录表
        tableEnv.executeSql(
            "CREATE TABLE user_login (" +
                "`pid` INT, " +
//                "`gid` INT, " +
//                "`p_mid` INT, " +
//                "`mid` INT, " +
                "`uid` INT, " +
//                "`reg_date` STRING, " +
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

        // 角色表
        tableEnv.executeSql(
            "CREATE TABLE role (" +
                "`pid` INT, " +
//                "`gid` INT, " +
//                "`p_mid` INT, " +
//                "`mid` INT, " +
                "`role_id` INT, " +
                "`created_date` STRING, " +
                "`created_time` BIGINT, " +
                "`ts` as TO_TIMESTAMP_LTZ(created_time, 3), " +
                "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
            ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'role'," +
                "'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "'properties.group.id' = 'my-group'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'" +
            ")"
        );

        // 充值表
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
                "`pid` INT, " +
//                "`gid` INT, " +
//                "`p_mid` INT, " +
//                "`mid` INT, " +
//                "`uid` INT, " +
                "`paid_money` DECIMAL, " +
//                "`create_date` STRING, " +
//                "`paid_date` STRING, " +
                "`notify_date` STRING, " +
                "`notify_time` BIGINT, " +
                "`ts` as TO_TIMESTAMP_LTZ(notify_time, 3), " +
                "WATERMARK FOR ts AS ts - INTERVAL '10' SECOND" +
            ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'order'," +
                "'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                "'properties.group.id' = 'my-group'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'json'" +
            ")"
        );

        // 联表

        String sql = "SELECT \n" +
            "    ul.pid as pid,\n" +
            "    ul.login_date as report_date,\n" +
            "    TUMBLE_START(ul.ts, INTERVAL '1' DAY) as window_start, \n" +
            "    TUMBLE_END(ul.ts, INTERVAL '1' DAY) as window_end, \n" +
            "    SUM(ul.dau) as dau,\n" +
            "    SUM(r.create_role) as create_role,\n" +
            "    SUM(o.sum_amount) as sum_amount\n" +
            "FROM (SELECT \n" +
            "        pid,\n" +
            "        login_date,\n" +
            "        MAX(ts) as ts,\n" +
            "        COUNT(DISTINCT uid) as dau\n" +
            "    FROM user_login\n" +
            "    GROUP BY pid, login_date) AS ul\n" +
            "FULL OUTER JOIN (SELECT \n" +
            "        pid,\n" +
            "        created_date,\n" +
            "        MAX(ts) as ts,\n" +
            "        COUNT(DISTINCT role_id) as create_role\n" +
            "    FROM role\n" +
            "    GROUP BY pid, created_date) AS r\n" +
            "ON ul.pid = r.pid AND r.created_date = ul.login_date\n" +
            "FULL OUTER JOIN (SELECT \n" +
            "        pid,\n" +
            "        notify_date,\n" +
            "        MAX(ts) as ts,\n" +
            "        SUM(paid_money) as sum_amount\n" +
            "    FROM orders\n" +
            "    GROUP BY pid, notify_date) AS o\n" +
            "ON o.pid = ul.pid AND o.notify_date = ul.login_date\n" +
            "GROUP BY TUMBLE(ul.ts, INTERVAL '1' DAY), ul.pid, ul.login_date";
        System.out.println(sql);
        Table totalData = tableEnv.sqlQuery(sql);
        tableEnv.toChangelogStream(totalData).print("totalData");


//        tableEnv.executeSql(
//            "create table operator (" +//                "`pid` int, " +
////                "`gid` int, " +
////                "`p_mid` int, " +
////                "`mid` int, " +
//                "`dau` bigint, " +
//                "`sum_amount` decimal, " +
////                "`new_sum_amount` decimal, " +
////                "`reg_date` string, " +
////                "`report_date` string, " +
////                "PRIMARY KEY (`pid`, `gid`, `p_mid`, `mid`, `reg_date`, `report_date`) NOT ENFORCED " +
//                "PRIMARY KEY (`pid`) NOT ENFORCED " +
//            ") with ( " +
//                "'connector' = 'upsert-kafka', " +
//                "'topic' = 'operator', " +
//                "'properties.bootstrap.servers' = '127.0.0.1:9092', " +
//                "'key.format' = 'json', " +
//                "'value.format' = 'json'" +
//            ")"
//        );
//
//        tableEnv.executeSql(
//            "insert into operator select " +
//                "pid, " +
////                "gid, " +
////                "p_mid, " +
////                "mid, " +
//                "dau, " +
//                "sum_amount " +
////                "new_sum_amount， " +
////                "reg_date, " +
////                "login_date as report_date " +
//            "from " + operatorData);

        env.execute();
    }
}
