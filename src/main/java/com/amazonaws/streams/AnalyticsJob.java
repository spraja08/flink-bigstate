/**
* The main DAG creator for entity analytics
*
* @author  Raja SP
*/

package com.amazonaws.steams;

import java.util.Properties;

import com.amazonaws.streams.operators.AnalyticsOperator;
import com.amazonaws.streams.sources.ConfigSource;
import com.amazonaws.streams.sources.EventSource;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticsJob {

	private static final Logger LOG = LoggerFactory.getLogger(AnalyticsJob.class);
	private static ParameterTool eaParameters = null;

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		loadParameters(args);

		Properties consumerConfig = new Properties();
		//populate configs here

		Properties producerConfig = new Properties();
		//populate configs here

		DataStream<JsonObject> condigUpdateStream = ConfigSource.createEntitySource(env, eaParameters);
		BroadcastStream<JsonObject> configUpdateBroadcastStream = condigUpdateStream
				.broadcast(Descriptors.configUpdateDescriptor);

		DataStream< JsonArray > routedStream = EventSource.getEventSource(env, eaParameters)
				.connect(configUpdateBroadcastStream)
				.process(new AnalyticsOperator(eaParameters))
				.uid("Analyser")
				.name("Analyser");
		env.execute("Streaming Analytics");
	}

	private static void loadParameters( String[] args) throws Exception {
		ParameterTool comamndLineParam = ParameterTool.fromArgs( args );
		String propsFile = comamndLineParam.get( "propertiesFile" );
		LOG.info( "Properties file name from command line : " + propsFile );
		eaParameters = ParameterTool.fromPropertiesFile( propsFile );
		StreamExecutionEnvironment.getExecutionEnvironment().getConfig().setGlobalJobParameters( eaParameters );
	}

	public static class Descriptors {
		public static final MapStateDescriptor<Void, JsonObject> configUpdateDescriptor =
			new MapStateDescriptor<>(
				"configUpdate", Types.VOID, TypeInformation.of(JsonObject.class));
	}	
}
