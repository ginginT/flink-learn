package com.mine.chaptor06;

import Utils.BitMap;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        KafkaSource<String> source = KafkaSource.<String>builder()
//            .setBootstrapServers("127.0.0.1:9092")
//            .setTopics("clicks")
//            .setGroupId("my-group1")
//            .setStartingOffsets(OffsetsInitializer.earliest())
//            .setValueOnlyDeserializer(new SimpleStringSchema())
//            .build();
//
//        DataStreamSource<String> stream = env.fromSource(
//            source,
//            WatermarkStrategy.noWatermarks(),
//            "Kafka Source"
//        );

        DataStreamSource<String> stream = env.socketTextStream("xiaoshaojiandeMac-mini.local", 8888);

        SingleOutputStreamOperator<JSONObject> map = stream
            .flatMap((String value, Collector<JSONObject> out) -> {
                if (value.equals("error")) {
                    throw new RuntimeException("error message ");
                }

                JSONObject jsonObject = JSONObject.parseObject(value);
                out.collect(jsonObject);
            })
            .returns(new TypeHint<>() {
            });

        map.print("stream");

        map.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                    .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (event, l) -> event.getLongValue("tm"))
            )
            .keyBy(value -> value.getString("pid") + value.getString("gid") + value.getString("p_mid") +
                value.getString("mid") + value.getString("login_date"))
            .window(TumblingEventTimeWindows.of(Time.days(1)))
            .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
            .aggregate(new UrlViewCountAgg(), new UrlViewCountResult())
            .print();

        env.execute();
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

            KeyedStateStore keyedState = context.windowState();
//            KeyedStateStore globalState = context.globalState();

            System.out.println(keyedState.toString());
//            System.out.println(globalState.toString());

            out.collect(s + " " + elements.iterator().next().get(s).counts + " windowStart=" + new Timestamp(start) + "windowEnd=" + new Timestamp(end));
        }
    }
}

