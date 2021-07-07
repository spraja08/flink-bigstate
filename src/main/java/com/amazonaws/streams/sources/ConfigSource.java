/**
* Source operator for reading entity definitions
*
* @author  Raja SP
*/

package com.amazonaws.streams.sources;

import java.util.Properties;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


public class ConfigSource {
    public static DataStream<JsonObject> createEntitySource(StreamExecutionEnvironment env, ParameterTool params) {
        String entitySource = params.get("ea.config.source");
        if (entitySource.equals("file")) {
            String filePath = params.get("ea.config.source.file.name");
            return env.readTextFile(filePath)
                    .map((stringEntitiesDef) -> (JsonObject) JsonParser.parseString(stringEntitiesDef))
                    .uid("EntitySourceFile").name("Entity Source - File");
        } else if (entitySource.equals("socket")) {
            return env
                    .socketTextStream(params.get("ea.config.source.socket.host"),
                            Integer.parseInt(params.get("ea.config.source.socket.port")), "\n", -1)
                    .map((stringEntitiesDef) -> (JsonObject) JsonParser.parseString(stringEntitiesDef))
                    .uid("EntitySourceSocket").name("Entity Source - Socket");
        } else if (entitySource.equals("kafka")) {
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", params.get("ea.kafka.bootstrap.servers"));
            properties.setProperty("group.id", "ea_config_consumer_group");
            return env.addSource(new FlinkKafkaConsumer<>(
                    params.get("ea.config.source.kafka.topic"),  new SimpleStringSchema(), properties))
                    .map( (configUpdate) -> ( JsonObject ) JsonParser.parseString (configUpdate))
                    .uid( "Confing Update" )
                    .name( "Config Update- Kafka ");
        }
        return null;          
    }
}