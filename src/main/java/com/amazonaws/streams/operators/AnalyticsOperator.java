/**
* Operator the handles the ADP and States computation
*
* @author  Raja SP
*/

package com.amazonaws.streams.operators;

import java.io.IOException;

import com.amazonaws.streams.engine.Analyser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.codehaus.commons.compiler.CompileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.streams.utils.Event;

public class AnalyticsOperator extends BroadcastProcessFunction<Event, JsonObject, JsonArray> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AnalyticsOperator.class);
    private Analyser api = null;
    private ParameterTool parameters = null;
    private int subTaskId;

    public AnalyticsOperator(ParameterTool eaParameters) throws CompileException, IOException {
        this.parameters = eaParameters;
	}

	@Override
    public void open(Configuration params) throws CompileException, IOException {
        api = new Analyser(parameters, this.subTaskId); 
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processBroadcastElement(JsonObject configUpdate,
            BroadcastProcessFunction<Event, JsonObject, JsonArray>.Context ctx, Collector<JsonArray> collector)
            throws Exception {
        String buildingBlock = configUpdate.get("buildingBlock").getAsString();
        LOG.info( "----------------- Recieved a config update : " + configUpdate.toString() + "------------------------"); 
        api.performConfigUpdate(configUpdate); 
    }

      @Override
      public void processElement(Event event,
              BroadcastProcessFunction<Event, JsonObject, JsonArray>.ReadOnlyContext ctx,
              Collector<JsonArray> collector) throws Exception {
          LOG.info( this.subTaskId + " : Recieved input  : " + event.toString() + "------------------------");         
          //implement the processing logic here
      }
}