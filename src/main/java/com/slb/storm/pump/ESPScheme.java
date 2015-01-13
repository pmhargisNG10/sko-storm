package com.slb.storm.pump;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.slb.common.data.DtsPacket;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

/**
 * Created by dpramodv on 11/14/14.
 */
public class ESPScheme implements Scheme {

    public static final String TRACE_KEY = "Trace";
    public static final String DEPTH_KEY = "Depth";
    public static final String TEMP_KEY = "Temp";
    public static final String DTS_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

//    public static final String PUMP_ID = "pumpId";
//    public static final String PUMP_ID_NO = "pumpIdNo";
//    public static final String TAG_ID = "tagId";
//    public static final String TAG_ID_NO = "tagIdNo";
//    public static final String EVENT_TIME = "eventTime";
//    public static final String VALUE = "value";
//    public static final String QUALITY = "quality";
//    public static final String COMMENT = "comment";

    private static final Gson GSON = new GsonBuilder().setDateFormat(DTS_DATE_FORMAT).create();

    //private static final long serialVersionUID = -2990121166902741545L;

    private static final Logger LOG = Logger.getLogger(ESPScheme.class);

    /**
     * {"TagId":"Accel_Pump_head_90","Time":"2014-10-16T18:25:12.0058824Z","DoubleValue":-3.4566017791799997,"Quality":0.0,"Comment":null}
     *
     * @param bytes
     * @return
     */
    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String dtsJson = new String(bytes, "UTF-8");
            //parse json string in
            DtsPacket dts = GSON.fromJson(dtsJson, DtsPacket.class);

            if(dts != null) {
                Date trace = dts.getTrace();
                long time = 0L;
                if(trace != null) {
                    time = trace.getTime();
                }
                double depth = dts.getDepth();
                double temp = dts.getTemp();
                return new Values(time, depth, temp);
            } else {
                LOG.error("JSON string from Kafka topic is empty. " + dtsJson);
                return null;
            }


        } catch (UnsupportedEncodingException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(TRACE_KEY,
                DEPTH_KEY,
                TEMP_KEY
        );

    }

    private String cleanup(String str) {
        if (str != null) {
            return str.trim().replace("\n", "").replace("\t", "");
        } else {
            return str;
        }

    }

}
