package com.hortonworks.common.data;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: phargis
 * Date: 1/11/15
 * Time: 8:12 PM
 */

public class DtsPacket implements Serializable {

    public static final String DTS_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private Date Trace;
    private Double Depth;
    private Double Temp;

    public Date getTrace() {
        return Trace;
    }

    public void setTrace(Date trace) {
        Trace = trace;
    }

    public Double getTemp() {
        return Temp;
    }

    public void setTemp(Double temp) {
        Temp = temp;
    }

    public Double getDepth() {
        return Depth;
    }

    public void setDepth(Double depth) {
        Depth = depth;
    }

    @Override public String toString() {
        StringBuilder result = new StringBuilder();
        result.append(this.getClass().getName() + " Object {");
        result.append(" \tTrace: " + Trace);
        result.append(" \tDepth: " + Depth);
        result.append(" \tTemp: " + Temp);
        result.append("}");

        return result.toString();
    }

    public static void main(String... args) {
        String json = "{\"Trace\":\"2013-05-05T14:17:22.000Z\",\"Depth\":1638.77958,\"Temp\":158.4924789313311}";
        //parse json
        Gson gson = new GsonBuilder().setDateFormat(DTS_DATE_FORMAT).create();

        DtsPacket dts = gson.fromJson(json, DtsPacket.class);
        System.out.println(dts);
    }
}
