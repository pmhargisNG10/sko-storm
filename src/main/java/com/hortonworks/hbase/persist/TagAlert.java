package com.hortonworks.hbase.persist;

import com.hortonworks.common.data.AlertTypeEnum;
import com.hortonworks.hbase.access.TimeStamp;
import com.hortonworks.hbase.rowkeys.AlertRowKey;
import com.hortonworks.storm.pump.ESPScheme;
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
        put.add(CF_TAG_ALERT, Bytes.toBytes(ESPScheme.TRACE_KEY), Bytes.toBytes(pumpIdNo));
        put.add(CF_TAG_ALERT, Bytes.toBytes(ESPScheme.DEPTH_KEY), Bytes.toBytes(tagId));
        put.add(CF_TAG_ALERT, Bytes.toBytes("alertType"), Bytes.toBytes(alertType.getType()));
        put.add(CF_TAG_ALERT, Bytes.toBytes("message"), Bytes.toBytes(message));
        alertTable.put(put);
    }
}
