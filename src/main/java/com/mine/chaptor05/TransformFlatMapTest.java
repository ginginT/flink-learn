package com.mine.chaptor05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("gin", "./tt", 10000L),
            new Event("ting", "./sii", 20000L),
            new Event("tt", "./qqq", 30000L)
        );

        SingleOutputStreamOperator<String> flatMap1 = stream.flatMap(new MyFlatMap());

        SingleOutputStreamOperator<String> flatMap2 = stream.flatMap(
            (Event value, Collector<String> out) -> {
                if (value.user.equals("gin")) {
                    out.collect(value.user);
                } else if (value.user.equals("ting")) {
                    out.collect(value.user);
                    out.collect(value.url);
                    out.collect(value.timestamp.toString());
                }
            }
        ).returns(new TypeHint<>() {});

        flatMap1.print("flatMap1");
        flatMap2.print("flatMap2");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
