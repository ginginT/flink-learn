package com.mine.chaptor05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("gin", "./home", 10000L),
            new Event("gin", "./about", 11000L),
            new Event("gin", "./pros?id=1", 12000L),
            new Event("ting", "./sii", 20000L),
            new Event("ting", "./sii", 12500L),
            new Event("gin", "./pros?id=3", 21000L),
            new Event("gin", "./pros?id=2", 13000L)
        );

        SingleOutputStreamOperator<Long> map = stream.map(new RichMapFunction<>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
            }

            @Override
            public Long map(Event value) {
                return value.timestamp;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
            }
        });

        map.print();

        env.execute();
    }
}
