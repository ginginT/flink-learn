package com.mine.chaptor06;

import com.mine.chaptor05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("127.0.0.1:9092")
            .setTopics("clicks")
            .setGroupId("my-group1")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStreamSource<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        SingleOutputStreamOperator<Event> map = stream
            .map(data -> {
                String[] field = data.split(",");
                return new Event(field[0].trim(), field[1].trim(), Long.valueOf(field[2].trim()));
            });
        map.print("stream");
        map.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp)
            )
            .keyBy(r -> r.url)
            .window(TumblingEventTimeWindows.of(Time.days(1)))
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
            .process(new WindowResult())
            .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Event, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Event> iterable, Collector<UrlViewCount> collector) {
            collector.collect(
                new UrlViewCount(
                    s,
                    // 获取迭代器中的元素个数
                    iterable.spliterator().getExactSizeIfKnown(),
                    context.window().getStart(),
                    context.window().getEnd()
                )
            );
        }
    }
}

