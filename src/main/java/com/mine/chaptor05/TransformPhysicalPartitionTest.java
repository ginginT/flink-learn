package com.mine.chaptor05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPhysicalPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
            new Event("gin", "./home", 10000L),
            new Event("ting", "./sii", 20000L),
            new Event("gin", "./about", 11000L),
            new Event("gin", "./pros?id=1", 12000L),
            new Event("ting", "./dfa", 12500L),
            new Event("gin", "./pros?id=3", 21000L),
            new Event("gin", "./pros?id=2", 13000L),
            new Event("gin", "./pros?id=4", 13000L)
        );

        // 1. 随机分区
        stream.shuffle().print().setParallelism(4);

        // 2. 轮询分区
//        stream.rebalance().print().setParallelism(4);

//        stream.print().setParallelism(4);

        // 3. 重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
                @Override
                public void run(SourceContext<Integer> ctx) {
                    for (int i = 1; i <= 8; i++) {
                        if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                            ctx.collect(i);
                        }
                    }
                }

                @Override
                public void cancel() {

                }
            })
            .setParallelism(2)
//            .rescale()
//            .print()
            .setParallelism(4);

        // 4. 广播
//        stream.broadcast().print().setParallelism(4);

        // 5. 全局分区
//        stream.global().print().setParallelism(4);

        // 6. 自定义重分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
            .partitionCustom(
                (Partitioner<Integer>) (key, numPartitions) -> key % 2,
                (KeySelector<Integer, Integer>) value -> value
            )
            .print()
            .setParallelism(4);

        env.execute();
    }
}
