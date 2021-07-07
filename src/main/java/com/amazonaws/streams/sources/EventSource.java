/**
* Source operator for consuming incoming events
*
* @author  Raja SP
*/

package com.amazonaws.streams.sources;

import java.util.Properties;
import com.amazonaws.streams.utils.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class EventSource {
    public static DataStream< Event > getEventSource( StreamExecutionEnvironment env, ParameterTool params  ) {
        String eventsSource = params.get("app.events.source");
        if( eventsSource.equals( "file" ) ) {
            String filePath = params.get( "app.events.source.file.name" );
            return env.readTextFile( filePath )
               .map( (stringEvent) -> new Event( stringEvent) )
               .uid( "EventsSource" )
               .name( "Events Source" );
        } else if( eventsSource.equals( "socket" ) ) {
            return env.socketTextStream(
                       params.get("app.events.source.socket.host"),
                       Integer.parseInt(params.get("app.events.source.socket.port")), "\n", -1)
                .map( (stringEvent) -> new Event( stringEvent) )
                .uid( "EventsSourceSocket" )
                .name( "Events Source - Socket" );       
        } else if (eventsSource.equals("kafka")) {
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", params.get("app.kafka.bootstrap.servers"));
            properties.setProperty("group.id", "ea_events_consumer_group");
            return env.addSource(new FlinkKafkaConsumer<>(
                    params.get("app.events.source.kafka.topic"),  new SimpleStringSchema(), properties))
                    .map( (stringEvent) -> new Event( stringEvent) )
                    .uid( "Events Stream" )
                    .name( "Events Streams - Kafka ");
        }
        return null;         
    }
}