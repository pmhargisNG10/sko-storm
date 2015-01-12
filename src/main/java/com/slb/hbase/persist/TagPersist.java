package com.slb.hbase.persist;

import java.io.IOException;
import java.text.ParseException;

import com.slb.hbase.connection.SingletonConnection;
import com.slb.hbase.rowkeys.TagRowKey;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TagPersist {

    private int hourrounder = 3600000;
    private final String tagtable = "t_tag_events";

    private final byte[] tagtablevaluefamilyb = "t".getBytes();
    private final byte[] tagtablecommentfamilyb = "c".getBytes();
    private final byte[] tagtablequalityfamilyb = "q".getBytes();

    private int cnt = 0;
    private final int flushfrequency = 50;
    private HTableInterface table = null;

    public TagPersist() {
        try {
            table = SingletonConnection.getInstance(tagtable);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write tags to tag_events table.
     * @param tagid
     * @param pumpid
     * @param timestamp
     * @param value
     * @param quality
     * @param comment
     * @return
     * @throws ParseException
     * @throws IOException
     */
    public byte[] writeTagValues(int tagid, int pumpid, long timestamp, double value, double quality, String comment )
            throws ParseException, IOException {

        long[] times = formattime(timestamp);
//        byte[] rk = new TagRowKey().constructRowkey1(pumpid, times[0], tagid);
        byte[] rk = new TagRowKey().constructRowkey(pumpid, times[0], tagid);

        Put put = new Put(rk);
        put.add(tagtablevaluefamilyb, Bytes.toBytes(times[1]), Bytes.toBytes(value));
        put.add(tagtablequalityfamilyb, Bytes.toBytes(times[1]), Bytes.toBytes(quality));
        if(null != comment)
            put.add(tagtablecommentfamilyb, Bytes.toBytes(times[1]), Bytes.toBytes(comment));

        table.put(put);


        if (cnt++ % flushfrequency == 0 ) {
            table.flushCommits();
        }

        return rk;

    }

    private long[] formattime(long timestamp) throws ParseException {
//        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'", Locale.ENGLISH).parse(timestamp);
        long timeinms = timestamp;

        long hourtime = timeinms - (timeinms % hourrounder);
        long minutesecondmstime = timeinms % hourrounder;

        long[] output = new long[2];

        output[0] = hourtime;
        output[1] = minutesecondmstime;

        return output;
    }

}

