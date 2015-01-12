package com.slb.hbase.rowkeys;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

public class MetaRowKey {

    // t_tagname OR m_pumpidtagid or p_pump
    public static final String TAG = "TAG";
    public static final String PUMP = "PUMP";
    public static final String MAPPING = "MAPPING";

    public static byte[] constructRowKey(String pump, String tag,  String type) {

        byte[] rowkey = null;

        if (PUMP.equalsIgnoreCase(type)) {
            rowkey =  Bytes.toBytes("P_" + pump);
        }

        if (TAG.equalsIgnoreCase(type)) {
            rowkey =   Bytes.toBytes("T_" + tag);
        }

        if(MAPPING.equalsIgnoreCase(type)) {
            rowkey = new byte[10];
            ByteBuffer bb = ByteBuffer.allocate(10);
            byte[] temp = Bytes.toBytes("M_");
            bb.put(temp);
            temp = Bytes.toBytes(Integer.parseInt(pump));
            bb.put(temp);
            temp = Bytes.toBytes(Integer.parseInt(tag));
            bb.put(temp);
            bb.rewind();
            bb.get(rowkey);
        }

        return rowkey;
    }

    public static int getTagIdFromMappingRowKey(byte[] rowKey) {
        //rowkey=M_12
        int tagId = 0;
        if(rowKey.length == 10) {
            tagId = Bytes.toInt(Bytes.tail(rowKey, 4));
        }
        return tagId;
    }

}

