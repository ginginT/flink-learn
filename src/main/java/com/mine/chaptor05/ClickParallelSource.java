package com.mine.chaptor05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickParallelSource implements ParallelSourceFunction<Event> {
    // 声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) {
        String[] users = {"ting", "gin"};
        String[] urls = {"./ts", "./home", "./about"};
        Random random = new Random();

        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            sourceContext.collect(new Event(user, url, Calendar.getInstance().getTimeInMillis()));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
