package com.mine.chaptor05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("gin", "./tt", 10000L),
            new Event("ting", "./sii", 20000L)
        );

        // 1. 使用自定义类，实现 MapFunction 接口
        SingleOutputStreamOperator<String> mapStr1 = stream.map(new MyMapper());

        // 2. 使用匿名类实现 MapFunction 接口
        SingleOutputStreamOperator<String> mapStr2 = stream.map(value -> value.url);

        // 3. 使用 lambda 表达式
        SingleOutputStreamOperator<String> mapStr3 = stream.map(data -> data.user);

        mapStr1.print("1");
        mapStr2.print("2");
        mapStr3.print("3");

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event, String> {
        @Override
        public String map(Event value) {
            return value.user;
        }
    }
}
