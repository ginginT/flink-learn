package com.mine.datasource;

import com.alibaba.fastjson.JSONObject;
import Utils.BitMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class Kafka {
    public static SingleOutputStreamOperator<String> getData(StreamExecutionEnvironment env) {
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("127.0.0.1:9092")
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

//        DataStreamSource<String> stream = env.socketTextStream("xiaoshaojiandeMac-mini.local", 8888);

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
                    .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (event, l) -> event.getLongValue("tm"))
            )
            .keyBy(value -> value.getString("pid") + value.getString("gid") + value.getString("p_mid") +
                value.getString("mid") + value.getString("login_date"))
            .window(TumblingEventTimeWindows.of(Time.days(1)))
            .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
            .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        aggregate.print("agg");

        return aggregate;
    }

    // 自定义增量聚合函数，来一条数据就加一
    public static class UrlViewCountAgg implements AggregateFunction<JSONObject, Map<String, Long>, Map<String, Long>> {
        public HashMap<String, BitMap> myMap = new HashMap<>();

        @Override
        public Map<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Long> add(JSONObject value, Map<String, Long> accumulator) {
            String key = value.getString("pid") + value.getString("gid") + value.getString("p_mid") +
                value.getString("mid") + value.getString("login_date");

            int uid = value.getIntValue("uid");

            Long counts = accumulator.getOrDefault(key, 0L);
            if (counts == 0L) {
                myMap.put(key, new BitMap(100000));
            }

            BitMap uidBit = myMap.get(key);
            if (!uidBit.contain(uid)) {
                uidBit.add(uid);
                accumulator.put(key, uidBit.counts);
                return accumulator;
            }

            return accumulator;
        }

        @Override
        public Map<String, Long> getResult(Map<String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Map<String, Long> merge(Map<String, Long> a, Map<String, Long> b) {
            return null;
        }
    }

    // 自定义窗口处理函数，只需要包装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Map<String, Long>, String, String, TimeWindow> {
        ValueState<Map<String, Long>> myValueState;

        @Override
        public void process(String s, Context context, Iterable<Map<String, Long>> elements, Collector<String> out) throws IOException {
            // 结合窗口信息，包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long counts = elements.iterator().next().get(s);

            KeyedStateStore keyedState = context.windowState();

//            Map<String, Long> state = myValueState.value();
//
//            if (state.isEmpty() || state.get(s) == null || !state.get(s).equals(counts)) {
//                state.put(s, counts);
//                myValueState.update(state);
//            } else {
//                return;
//            }

            out.collect(s + " " + counts + " windowStart=" + new Timestamp(start) + "windowEnd=" + new Timestamp(end));
        }

//        @Override
//        public void open(Configuration parameters) {
//            myValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("my state", Types.MAP(Types.STRING, Types.LONG)));
//        }
    }
}
