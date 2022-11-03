package com.mine.datasource;

import com.alibaba.fastjson.JSONObject;
import Utils.BitMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
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

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class Kafka {
    public static SingleOutputStreamOperator<String> getData(StreamExecutionEnvironment env) {
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("106.55.198.234:9093,106.55.198.234:9094")
            .setTopics("first")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStreamSource<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        SingleOutputStreamOperator<JSONObject> flatMap = stream
            .flatMap((String value, Collector<JSONObject> out) -> {
                if (value.equals("error")) {
                    throw new RuntimeException("error message ");
                }

                JSONObject jsonObject = JSONObject.parseObject(value);
                out.collect(jsonObject);
            })
            .returns(new TypeHint<>() {
            });
        flatMap.print("stream");

        SingleOutputStreamOperator<String> aggregate = flatMap
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                    .withTimestampAssigner((element, recordTimestamp) -> element.getLongValue("tm"))
            )
            .keyBy(value -> value.getString("pid") + value.getString("gid") + value.getString("p_mid") + value.getString("mid") + value.getString("login_date"))
            .window(TumblingEventTimeWindows.of(Time.minutes(10)))
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
            .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        aggregate.print("agg");

        return aggregate;
    }

    // 自定义增量聚合函数，来一条数据就加一
    public static class UrlViewCountAgg implements AggregateFunction<JSONObject, Map<String, BitMap>, Map<String, BitMap>> {
        @Override
        public Map<String, BitMap> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, BitMap> add(JSONObject value, Map<String, BitMap> accumulator) {
            String key = value.getString("pid") + value.getString("gid") + value.getString("p_mid") +
                value.getString("mid") + value.getString("login_date");

            int uid = value.getIntValue("uid");

            BitMap uidBit = accumulator.getOrDefault(key, new BitMap(100000));

            if (!uidBit.contain(uid)) {
                uidBit.add(uid);

                accumulator.put(key, uidBit);
                return accumulator;
            }

            return accumulator;
        }

        @Override
        public Map<String, BitMap> getResult(Map<String, BitMap> accumulator) {
            return accumulator;
        }

        @Override
        public Map<String, BitMap> merge(Map<String, BitMap> a, Map<String, BitMap> b) {
            return null;
        }
    }

    // 自定义窗口处理函数，只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Map<String, BitMap>, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Map<String, BitMap>> elements, Collector<String> out) {
            // 结合窗口信息，包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();

            out.collect(s + " " + elements.iterator().next().get(s).counts + " windowStart=" + new Timestamp(start) + "windowEnd=" + new Timestamp(end));
        }
    }
}
