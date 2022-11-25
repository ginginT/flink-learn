package com.mine.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ToCkTableJoin {
    public static void main(String[] args) throws Exception {
        Configuration conf  = new Configuration();
//        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//        //自定义端口
//        conf.setInteger(RestOptions.PORT, 8081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        conf.setString("table.exec.state.ttl", "5 s");

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

        // 用户登录表
        tableEnv.executeSql(
            "CREATE TABLE user_login (" +
                "`pid` INT, " +
//                "`gid` INT, " +
//                "`p_mid` INT, " +
//                "`mid` INT, " +
                "`uid` INT, " +
//                "`reg_date` STRING, " +
//                "`login_date` STRING, " +
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
//                "`notify_date` STRING, " +
                "`notify_time` BIGINT, " +
//                "`ts` TIMESTAMP(3) METADATA FROM 'timestamp', " +
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

        String sql =
            "SELECT \n" +
            "    *\n" +
            "FROM (\n" +
            "    SELECT \n" +
            "        pid, \n" +
//            "        gid, \n" +
//            "        p_mid, \n" +
//            "        mid, \n" +
            "        SUM(paid_money) AS sum_amount, \n" +
//            "        IF(create_date = notify_date, SUM(paid_money), 0) AS new_sum_amount, \n" +
//            "        create_date, \n" +
//            "        notify_date, \n" +
            "        window_start, \n" +
            "        window_end \n" +
            "    FROM TABLE(\n" +
            "        CUMULATE( TABLE orders, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' DAY )\n" +
            "    )" +
            "    GROUP BY \n" +
//            "        pid, gid, p_mid, mid, create_date, notify_date, window_start, window_end\n" +
            "        pid, window_start, window_end\n" +
            ") AS o\n" +
            "FULL OUTER JOIN (\n" +
            "    SELECT \n" +
            "        pid, \n" +
//            "        gid, \n" +
//            "        p_mid, \n" +
//            "        mid, \n" +
            "        COUNT(DISTINCT uid) as dau, \n" +
//            "        reg_date, \n" +
//            "        login_date, \n" +
            "        window_start, \n" +
            "        window_end \n" +
            "    FROM TABLE(\n" +
            "        CUMULATE( TABLE user_login, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' DAY )\n" +
            "    )" +
            "    GROUP BY \n" +
//            "        pid, gid, p_mid, mid, reg_date, login_date, window_start, window_end\n" +
            "        pid, window_start, window_end\n" +
            ") AS ul\n" +
            "ON o.pid = ul.pid";
//            "ON o.pid = ul.pid AND o.gid = ul.gid AND o.p_mid = ul.p_mid AND o.mid = ul.mid\n" +
//            "    AND o.create_date = ul.reg_date \n" +
//            "    AND o.notify_date = ul.login_date";
        System.out.println(sql);
        Table operatorData = tableEnv.sqlQuery(sql);
        tableEnv.toChangelogStream(operatorData).print("operatorData");

        tableEnv.executeSql(
            "create table operator (" +
                "`pid` int, " +
//                "`gid` int, " +
//                "`p_mid` int, " +
//                "`mid` int, " +
                "`dau` bigint, " +
                "`sum_amount` decimal, " +
//                "`new_sum_amount` decimal, " +
//                "`reg_date` string, " +
//                "`report_date` string, " +
//                "PRIMARY KEY (`pid`, `gid`, `p_mid`, `mid`, `reg_date`, `report_date`) NOT ENFORCED " +
                "PRIMARY KEY (`pid`) NOT ENFORCED " +
            ") with ( " +
                "'connector' = 'upsert-kafka', " +
                "'topic' = 'operator', " +
                "'properties.bootstrap.servers' = '127.0.0.1:9092', " +
                "'key.format' = 'json', " +
                "'value.format' = 'json'" +
            ")"
        );

        tableEnv.executeSql(
            "insert into operator select " +
                "pid, " +
//                "gid, " +
//                "p_mid, " +
//                "mid, " +
                "dau, " +
                "sum_amount " +
//                "new_sum_amount， " +
//                "reg_date, " +
//                "login_date as report_date " +
            "from " + operatorData);

        env.execute();
    }
}
