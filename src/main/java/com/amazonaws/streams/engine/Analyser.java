/**
* Computes all the ADPs and States for the incoming event
*
* @author  Raja SP
*/

package com.amazonaws.streams.engine;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.codehaus.commons.compiler.CompileException;
import com.amazonaws.streams.utils.LFUCache;

public class Analyser implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Analyser.class);
    private LFUCache cache =  null;

    private int subTaskId;

    public Analyser(ParameterTool parameters, int subTaskId) throws CompileException, IOException {
        this.subTaskId = subTaskId;
        cache = new LFUCache( parameters.getInt( "capacity" ) );
    }

    public void performConfigUpdate( JsonObject config ) throws CompileException, IOException {
        //perform config update here
        cache.put( config.get( "key" ).getAsInt(), config.get( "value").getAsInt() );
    }

    public JsonObject process(JsonObject inputEvent, String entityType, String entityId, JsonObject keyObject)
            throws InvocationTargetException, IOException {
        //perform event processing here 
        return null;       
    }
}