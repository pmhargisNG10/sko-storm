package com.slb.hbase.rowkeys;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class TagRowKey {
    private static Logger log = Logger.getLogger(TagRowKey.class);

    ByteBuffer bb = ByteBuffer.allocate(16);

    public byte[] constructRowkey(int pumpid, long ts, int tagid) {
        //This will create a byte[] of pumpid, tagid, ts
        byte[] rowkey = new byte[16];
        bb.clear().rewind();
        bb.putInt(pumpid);
        bb.putLong(ts);
        bb.putInt(tagid);
        bb.rewind();
        bb.get(rowkey);
        return rowkey;
    }


    public byte[] constructRowkey1(int pumpid, long ts, int tagid) {
        //This will create a byte[] of pumpid, tagid, ts
        String key = pumpid + "|" + ts + "|" + tagid;
        return Bytes.toBytes(key);
    }


    public long[] deconstructRowkey(byte[] rowkey) {
        bb.clear().rewind();
        bb.put(rowkey);

        long[] output = new long[3];

//		int pumpid = bb.getInt();
//		long ts = bb.getLong();
//		int tag = bb.getInt();

        output[0] = bb.getInt();
        output[1] = bb.getLong();
        output[2] = bb.getInt();

        //OR

//		pumpid = Bytes.toInt(rowkey, 0);
//		ts = Bytes.toLong(rowkey, 4);
//		tag = Bytes.toInt(rowkey, 12);

        output[0] = Bytes.toInt(rowkey, 0);
        output[1] = Bytes.toLong(rowkey, 4);
        output[2] = Bytes.toInt(rowkey, 12);

        return output;
    }

    public long[] deconstructRowkey1(byte[] rowkey) {
        long[] output = new long[3];

//		int pumpid = bb.getInt();
//		long ts = bb.getLong();
//		int tag = bb.getInt();

//        output[0] = Bytes.toLong(Bytes.head(rowkey, 4));
//        output[1] = Bytes.toLong(rowkey, 8);
//        output[2] = Bytes.toLong(Bytes.tail(rowkey, 4));;

        output[0] = Bytes.toInt(rowkey, 0);
        output[1] = Bytes.toLong(rowkey, 4);
        output[2] = Bytes.toInt(rowkey, 12);

        log.info("output[0] = " + output[0]);
        log.info("output[1] = " + output[1]);
        log.info("output[2] = " + output[2]);

        return output;
    }

}