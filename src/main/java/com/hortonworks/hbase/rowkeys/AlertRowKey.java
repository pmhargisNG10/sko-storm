package com.hortonworks.hbase.rowkeys;

import com.hortonworks.common.data.AlertTypeEnum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Created by dpramodv on 12/4/14.
 */
public class AlertRowKey {
    private static final Logger LOG = Logger.getLogger(AlertRowKey.class);

    /**
     * construct row key
     * @param tagId
     * @param pumpId
     * @param alertType
     * @return
     */
    public static byte[] constructRowKey(int pumpId, String tagId, AlertTypeEnum alertType, String time){
        StringBuilder rowKey = new StringBuilder();
        rowKey.append(alertType.getType()).append("_").append(pumpId).append("-");
        if(StringUtils.isNotEmpty(tagId)) {
            rowKey.append(tagId).append("-").append(time);
        }
        return Bytes.toBytes(rowKey.toString());
    }

}
