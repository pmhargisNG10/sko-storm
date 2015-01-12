package com.slb.hbase.persist;

import com.slb.common.data.AlertTypeEnum;
import com.slb.hbase.access.TimeStamp;
import com.slb.hbase.rowkeys.AlertRowKey;
import com.slb.storm.pump.ESPScheme;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by dpramodv on 12/3/14.
 */
public class TagAlert {
    //TABLE
    public static final String TAG_ALERTS_TABLE_NAME =  "t_tag_alerts";
    //CF
    public static final byte[] CF_TAG_ALERT = Bytes.toBytes("a");

    public void put(String tagId, int pumpIdNo, AlertTypeEnum alertType,
                             String message, HTableInterface alertTable) throws IOException {
        String currentTime = TimeStamp.getTimeStamp();
        byte[] rowKey = AlertRowKey.constructRowKey(pumpIdNo, tagId, alertType, currentTime);
        Put put = new Put(rowKey);
        put.add(CF_TAG_ALERT, Bytes.toBytes(ESPScheme.PUMP_ID_NO), Bytes.toBytes(pumpIdNo));
        put.add(CF_TAG_ALERT, Bytes.toBytes(ESPScheme.TAG_ID), Bytes.toBytes(tagId));
        put.add(CF_TAG_ALERT, Bytes.toBytes("alertType"), Bytes.toBytes(alertType.getType()));
        put.add(CF_TAG_ALERT, Bytes.toBytes("message"), Bytes.toBytes(message));
        alertTable.put(put);
    }
}
