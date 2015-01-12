package com.slb.storm.pump;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.slb.common.data.TagEvent;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

/**
 * Created by dpramodv on 11/14/14.
 */
public class ESPScheme implements Scheme {

    public static final String PUMP_ID = "pumpId";
    public static final String PUMP_ID_NO = "pumpIdNo";
    public static final String TAG_ID = "tagId";
    public static final String TAG_ID_NO = "tagIdNo";
    public static final String EVENT_TIME = "eventTime";
    public static final String VALUE = "value";
    public static final String QUALITY = "quality";
    public static final String COMMENT = "comment";

    private static final Gson GSON = new
            GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'").create();

    //private static final long serialVersionUID = -2990121166902741545L;

    private static final Logger LOG = Logger.getLogger(ESPScheme.class);

    /**
     * tag_id | timestamp | m1_Qualifier | m1_Value
     *
     * @param bytes
     * @return
     */
    public List<Object> deserialize1(byte[] bytes) {
        try {
            String tagEvent = new String(bytes, "UTF-8");
            //parse json string in
            String[] pieces = tagEvent.split("\\|");

            String tagId = pieces[0];
//            Timestamp eventTime = Timestamp.valueOf(pieces[1]);
//            Timestamp eventTime = new Timestamp(Long.parseLong(pieces[1]));
            String eventTime = pieces[1];
            String metricType = pieces[2];
            String metricValue = pieces[3];
            return new Values(cleanup(metricType), cleanup(tagId),
                    cleanup(eventTime), cleanup(metricValue));

        } catch (UnsupportedEncodingException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {"TagId":"Accel_Pump_head_90","Time":"2014-10-16T18:25:12.0058824Z","DoubleValue":-3.4566017791799997,"Quality":0.0,"Comment":null}
     *
     * @param bytes
     * @return
     */
    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String tagEventStr = new String(bytes, "UTF-8");
            //parse json string in
            TagEvent tagEvent = GSON.fromJson(tagEventStr, TagEvent.class);

            if(tagEvent != null) {
                String tagId = tagEvent.getTagId();
                Date date = tagEvent.getTime();
                long time = 0L;
                if(date != null) {
                    time = date.getTime();
                }
                double value = tagEvent.getDoubleValue();
                double quality = tagEvent.getQuality();
                String comment = tagEvent.getComment();
                return new Values(cleanup(tagId), time,
                        value, quality, cleanup(comment));
            } else {
                LOG.error("JSON string from Kafka topic is empty. " + tagEventStr);
                return null;
            }


        } catch (UnsupportedEncodingException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(TAG_ID,
                EVENT_TIME,
                VALUE,
                QUALITY,
                COMMENT
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
