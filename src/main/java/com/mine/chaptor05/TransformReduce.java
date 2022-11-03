package com.mine.chaptor05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("gin", "./home", 10000L),
            new Event("gin", "./about", 11000L),
            new Event("ting", "./sii", 20000L),
            new Event("ting", "./sii", 12500L),
            new Event("gin", "./pros?id=1", 12000L),
            new Event("ting", "./sii", 12500L),
            new Event("gin", "./pros?id=3", 21000L),
            new Event("gin", "./pros?id=2", 13000L)
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> result = stream.map(
                (MapFunction<Event, Tuple2<String, Long>>) value -> Tuple2.of(value.user, 1L)
            )
            .returns(new TypeHint<>() {})
            .keyBy(data -> data.f0)
            .reduce(
                (ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1)
            )
            .keyBy(data -> true)
            .reduce(
                (ReduceFunction<Tuple2<String, Long>>) (value1, value2) -> value1.f1 > value2.f1 ? value1 : value2
            );

        result.print();

        env.execute();
    }
}
