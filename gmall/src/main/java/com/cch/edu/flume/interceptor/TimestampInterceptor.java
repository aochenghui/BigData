package com.cch.edu.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //1、获取header和body的数据
        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);
        try {
            JSONObject jsonObject = JSONObject.parseObject(log);

            String ts = jsonObject.getString("ts");
            headers.put("timestamp", ts);
            return event;
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()) {
            Event event = iterator.next();
            if (intercept(event) == null) {
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
