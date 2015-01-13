package com.slb.common.data;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by dpramodv on 11/25/14.
 */

public class TagEvent implements Serializable {
    private String TagId;
    private Date Time;
    private Double DoubleValue;
    private Double Quality;
    private String Comment;
    public String getTagId() {
        return TagId;
    }
    public void setTagId(String tagId) {
        TagId = tagId;
    }
    public Date getTime() {
        return Time;
    }
    public void setTime(Date time) {
        Time = time;
    }
    public Double getDoubleValue() {
        return DoubleValue;
    }
    public void setDoubleValue(Double doubleValue) {
        DoubleValue = doubleValue;
    }
    public Double getQuality() {
        return Quality;
    }
    public void setQuality(Double quality) {
        Quality = quality;
    }
    public String getComment() {
        return Comment;
    }
    public void setComment(String comment) {
        Comment = comment;
    }

    @Override public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(this.getClass().getName() + " Object {");
        result.append(" Tag Id: " + TagId);
        result.append(" \tAt time: " + Time.getTime());
        result.append(" \tValud: " + DoubleValue );
        result.append(" \tQuality: " + Quality);
        result.append(" \tComment: " + Comment);
        result.append("}");

        return result.toString();
    }

    public static void main(String... args) {
        String jsonString = "{\"TagId\":\"Accel_Pump_head_90\",\"Time\":\"2014-10-16T18:25:12.0058824Z\",\"DoubleValue\":-3.4566017791799997,\"Quality\":0.0,\"Comment\":null}";
        //parse json
        Gson gson = new
                GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").create();

        TagEvent tagEvent = gson.fromJson(jsonString, TagEvent.class);
        System.out.println(tagEvent);
    }
}
