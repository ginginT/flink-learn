package com.mine.chaptor05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("gin", "./tt", 10000L),
            new Event("ting", "./sii", 20000L)
        );

        SingleOutputStreamOperator<Event> filterEvent1 = stream.filter(new MyFilter());

        SingleOutputStreamOperator<Event> filterEvent2 = stream.filter(value -> value.user.equals("gin"));

        SingleOutputStreamOperator<Event> filterEvent3 = stream.filter(data -> data.user.equals("ting"));

        filterEvent1.print("filterEvent1");
        filterEvent2.print("filterEvent2");
        filterEvent3.print("filterEvent3");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event value) {
            return value.user.equals("ting");
        }
    }
}
