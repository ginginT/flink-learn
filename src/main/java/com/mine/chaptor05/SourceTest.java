package com.mine.chaptor05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 读取有界流
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件读取
        DataStreamSource<String> boundedStream1 = env.readTextFile("input/clicks.txt");

        // 2. 从集合中读取
        ArrayList<Object> arrList1 = new ArrayList<>();
        arrList1.add(2);
        arrList1.add(5);
        DataStreamSource<Object> boundedStream2 = env.fromCollection(arrList1);

        ArrayList<Object> arrList2 = new ArrayList<>();
        arrList2.add(new Event("gin", "./tt", 10000L));
        arrList2.add(new Event("ting", "./sii", 20000L));
        DataStreamSource<Object> boundedStream3 = env.fromCollection(arrList2);

        // 3. 从元素中读取
        DataStreamSource<Event> boundedStream4 = env.fromElements(
                new Event("gin", "./tt", 10000L),
                new Event("ting", "./sii", 20000L)
        );

        boundedStream1.print("boundedStream1");
        boundedStream2.print("boundedStream2");
        boundedStream3.print("boundedStream3");
        boundedStream3.print("boundedStream4");

        env.execute();
    }
}
