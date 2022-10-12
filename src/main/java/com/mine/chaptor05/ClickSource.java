package com.mine.chaptor05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    // 声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Mary", "gin", "ting"};
        String[] urls = {"./home", "./about", "./ts"};
        Random random = new Random();

        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            sourceContext.collect(new Event(user, url, Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
