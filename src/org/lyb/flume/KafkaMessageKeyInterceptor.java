package org.lyb.flume;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 *   该拦截器将kafka message的key按照指定分隔符分隔成一个一个的event header key,在sink中可以通过%{}方式来引用
 */
public class KafkaMessageKeyInterceptor implements Interceptor {

    private final String splitChar;

    /**
     * Only {@link com.hupu.dace.flume.interceptors.KafkaMessageKeyInterceptor.Builder} can build me
     */
    private KafkaMessageKeyInterceptor(String splitChar) {
        this.splitChar = splitChar;
    }

    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        if (headers.containsKey(Constants.KAFKA_MESSAGE_KEY)) {
            String kafkaMessageKey = headers.get(Constants.KAFKA_MESSAGE_KEY); //从header中获取kafka message key
            String[] values = kafkaMessageKey.split(splitChar); //按照指定的分隔符将key拆分

            for (int i = 0; i < values.length; i++) {
                headers.put(Constants.SPLIT_KEY_PREFIX + i, values[i]);//拆分出来的每一个片段，按指定的前缀加上序号，写入event header
            }
            return event;

        } else {
            return null;
        }
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     *
     * @param events
     * @return
     */
    public List<Event> intercept(List<Event> events) {
        List<Event> out = Lists.newArrayList();
        for (Event event : events) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    public void close() {
        // no-op
    }


    public static class Builder implements Interceptor.Builder {

        private String splitChar = Constants.DEFAULT_SPLIT_CHAR;

        public Interceptor build() {
            return new KafkaMessageKeyInterceptor(splitChar);
        }

        public void configure(Context context) {
            splitChar = context.getString(Constants.SPLIT_CHAR, Constants.DEFAULT_SPLIT_CHAR);
        }
    }

    public static class Constants {
        public static String KAFKA_MESSAGE_KEY = "key";   //kafka source会将message key写进event header中一个叫"key"的header
        public static String SPLIT_CHAR = "split";
        public static String DEFAULT_SPLIT_CHAR = ":";
        public static String SPLIT_KEY_PREFIX = "s";
    }

}