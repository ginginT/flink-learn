package com.mine.chaptor05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这种方式由于吞吐量小、稳定性较差，一般也是用于测试。
        DataStreamSource<String> stream = env.socketTextStream("localhost", 7777);

        stream.print("stream");

        env.execute();
    }
}
