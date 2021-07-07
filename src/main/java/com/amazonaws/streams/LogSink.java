/**
* Log-Sink utility
*
* @author  Raja SP
*/

package com.amazonaws.streams;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSink<T> implements SinkFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LogSink.class);

    public LogSink() {
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value != null) {
            LOG.warn("------------Sent to Log" + value.toString() + "------------");
        }
    }
}