package com.slb.common.data;

import com.slb.storm.pump.ESPScheme;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by dpramodv on 12/4/14.
 */
public class Alert {
    public static final byte[] ALERT_TYPE = Bytes.toBytes("alertType");
    public static final byte[] MESSAGE = Bytes.toBytes("message");

    private String alertType;
    private String message;
    private TagMetaData tagMetaData;

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public TagMetaData getTagMetaData() {
        return tagMetaData;
    }

    public void setTagMetaData(TagMetaData tagMetaData) {
        this.tagMetaData = tagMetaData;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append("Alert Type:").append(this.alertType);
        if(getTagMetaData() != null) builder.append("|Tag:").append(getTagMetaData().getTag());
        builder.append("|Message:").append(this.message);

        return builder.toString();
    }
}
