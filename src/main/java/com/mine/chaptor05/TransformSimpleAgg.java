package com.mine.chaptor05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("gin", "./home", 10000L),
            new Event("gin", "./about", 11000L),
            new Event("gin", "./pros?id=1", 12000L),
            new Event("ting", "./sii", 20000L),
            new Event("gin", "./pros?id=3", 21000L),
            new Event("ting", "./sii", 12500L),
            new Event("gin", "./pros?id=2", 13000L)
        );

        SingleOutputStreamOperator<Event> max = stream.keyBy(value -> value.user).max("timestamp");

        SingleOutputStreamOperator<Event> maxBy = stream.keyBy(data -> data.user).maxBy("timestamp");

        max.print("max: ");
        maxBy.print("maxBy: ");

        env.execute();
    }
}
