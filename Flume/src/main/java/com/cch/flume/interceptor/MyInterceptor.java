package com.cch.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    public void initialize() {

    }

    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();

        byte[] body = event.getBody();
        String data = new String(body);

        if (data.contains("atguigu")){
            headers.put("title", "at");
        } else if (data.contains("cch")) {
            headers.put("title", "sh");
        }else {
            headers.put("title", "ot");
        }
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {

    }

    /**
     * 用于构建拦截器实例
     */
    public static class MyBuilder implements Builder{

        public Interceptor build() {
            return new MyInterceptor();
        }

        /*
         * 此方法中可以重新定义设置Flume中参数
         * */
        public void configure(Context context) {

        }
    }
}
