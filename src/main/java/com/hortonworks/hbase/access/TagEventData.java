package com.hortonworks.hbase.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.hortonworks.common.data.TagEvent;
import com.hortonworks.hbase.connection.SingletonConnection;
import com.hortonworks.hbase.lookup.MetaLookup;
import com.hortonworks.hbase.rowkeys.MetaRowKey;
import com.hortonworks.hbase.rowkeys.TagRowKey;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Created by dpramodv on 11/30/14.
 */
public class TagEventData {
    private static Logger log = Logger.getLogger(TagEventData.class);


    private int hourrounder = 3600000;
    public static final String TBL_TAG_META_DATA = "t_tag_meta_data";
    public static final String TBL_TAG_EVENTS = "t_tag_events";
    public static final byte[] CF_TAG_META_DATA = "m".getBytes();
    public static final byte[] pumpidb = "pumpIdNo".getBytes();

    public static final byte[] CF_EVENTS_TE = Bytes.toBytes("t");
    public static final byte[] CF_QUALITY_TE = Bytes.toBytes("q");
    public static final byte[] CF_COMMENT_TE = Bytes.toBytes("c");

    private final byte[] mask = new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0};

    public HashMap<String, TagEvent> getAllDataForAPump(String pumpname, String starttime, String endtime) throws IOException, ParseException {

        //1. Lookup meta data table for pumpId corresponding to pump name.
        //2. Take starttime and endtime and decompose it into offset and rowkey hour.
        //3. Setup scanner
        //4. Execute to get results.

        Scan scan = setupBaseScan(pumpname, starttime, endtime);
        scan.addFamily(CF_EVENTS_TE);
        HTableInterface tagtable = SingletonConnection.getInstance(TBL_TAG_EVENTS);
        ResultScanner rst = tagtable.getScanner(scan);

        //todo: CF and CQ filters
//         Filter
//        Tag
        //iterate through to get the values.
        for(Result rr = rst.next(); rr != null; rr = rst.next()){
            //Not coming here
            for(KeyValue keyValue : rr.list()) {
                keyValue.getKey();
                System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()));
            }
        }

        return null;

    }


    public HashMap<String, TagEvent> getDataForSpecificTagsForAPump(String pumpname, List<String> tags, String starttime, String endtime) throws IOException, ParseException {

        //1. Lookup meta data table for pumpId corresponding to pump name.
        //2. Take starttime and endtime and decompose it into offset and rowkey hour.
        //3. Setup scanner
        //4. Execute to get results.

        Scan scan = setupBaseScan(pumpname, starttime, endtime);

        FilterList fbaselist = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        List<Integer> tagids = new ArrayList<Integer>();

        for (String tag : tags) {

            int tagid = 0;
            //Write code to lookup a tagid for a given tag
            tagids.add(tagid);
        }

        fbaselist.addFilter(setupFuzzyFilter(tagids, new byte[16]));

        FilterList fCQLowFilters =  new FilterList(FilterList.Operator.MUST_PASS_ONE);

        RowFilter rklowFilter = new RowFilter(CompareOp.GREATER, new BinaryComparator(scan.getStartRow()));
        fCQLowFilters.addFilter(rklowFilter);
        QualifierFilter qflowfilter = new QualifierFilter(CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(formattime(dateToMillis(starttime))[1])));
        fCQLowFilters.addFilter(qflowfilter);

        fbaselist.addFilter(fCQLowFilters);


        FilterList fCQHighFilters =  new FilterList(FilterList.Operator.MUST_PASS_ONE);

        RowFilter rkhighFilter = new RowFilter(CompareOp.LESS, new BinaryComparator(scan.getStopRow()));
        fCQHighFilters.addFilter(rkhighFilter);
        QualifierFilter qfhighfilter = new QualifierFilter(CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(formattime(dateToMillis(endtime))[1])));
        fCQHighFilters.addFilter(qfhighfilter);

        fbaselist.addFilter(fCQHighFilters);

        scan.setFilter(fbaselist);

        HTableInterface tagtable = SingletonConnection.getInstance(TBL_TAG_EVENTS);
        ResultScanner rst = tagtable.getScanner(scan);

        //iterate through to get the values.

        return null;

    }
    //2. Lookup meta data table for all tagids associated to this pumpid and store it in a collection
    //3.

    public HashMap<String, TagEvent> getDataForSpecificTagForAPump(String pumpname, String tag, String starttime, String endtime) throws IOException, ParseException {
        //1. Lookup meta data table for pumpId corresponding to pump name.
        //2. Take starttime and endtime and decompose it into offset and rowkey hour.
        //3. Setup scanner
        //4. Execute to get results.

        Scan scan = setupBaseScan(pumpname, starttime, endtime);

        List<Integer> tagids = new ArrayList<Integer>();
        int tagid = 0;
        //Write code to lookup a tagid for a given tag
        tagids.add(tagid);

        FilterList fbaselist = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        fbaselist.addFilter(setupFuzzyFilter(tagids, new byte[16]));

        FilterList fCQLowFilters =  new FilterList(FilterList.Operator.MUST_PASS_ONE);

        RowFilter rklowFilter = new RowFilter(CompareOp.GREATER, new BinaryComparator(scan.getStartRow()));
        fCQLowFilters.addFilter(rklowFilter);
        QualifierFilter qflowfilter = new QualifierFilter(CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(formattime(dateToMillis(starttime))[1])));
        fCQLowFilters.addFilter(qflowfilter);

        fbaselist.addFilter(fCQLowFilters);


        FilterList fCQHighFilters =  new FilterList(FilterList.Operator.MUST_PASS_ONE);

        RowFilter rkhighFilter = new RowFilter(CompareOp.LESS, new BinaryComparator(scan.getStopRow()));
        fCQHighFilters.addFilter(rkhighFilter);
        QualifierFilter qfhighfilter = new QualifierFilter(CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes(formattime(dateToMillis(endtime))[1])));
        fCQHighFilters.addFilter(qfhighfilter);

        fbaselist.addFilter(fCQHighFilters);


        scan.setFilter(fbaselist);
        HTableInterface tagtable = SingletonConnection.getInstance(TBL_TAG_EVENTS);
        ResultScanner rst = tagtable.getScanner(scan);

        //todo: iterate through to get the values.

        return null;
    }


    private long[] formattime(long timestamp) throws ParseException {

//        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'", Locale.ENGLISH).parse(timestamp);
//        long timeinms = date.getTime();
        long timeinms = timestamp;

        long hourtime = timeinms - (timeinms % hourrounder);
        long minutesecondmstime = timeinms % hourrounder;

        long[] output = new long[2];

        output[0] = hourtime;
        output[1] = minutesecondmstime;

        return output;
    }

    private long dateToMillis(String time) {
        DateTime dateTime = new DateTime(time);
        GregorianCalendar calendar = dateTime.toDateTime(DateTimeZone.UTC).toGregorianCalendar();
        return calendar.getTimeInMillis();
    }

    private Scan setupBaseScan(String pumpname, String starttime, String endtime) throws IOException, ParseException {

        byte[] rk = new MetaRowKey().constructRowKey(pumpname, "", MetaRowKey.PUMP);
        Get get = new Get(rk);
        HTableInterface metatable = SingletonConnection.getInstance(TBL_TAG_META_DATA);


        Result rs = metatable.get(get);
        byte[] pumpid = null;
        if (rs != null) {
            //TagEvent ID FOLLOWED BY PUMP ID
            pumpid = rs.getValue(CF_TAG_META_DATA, pumpidb);
        }


        long[] stimes = formattime(dateToMillis(starttime));
        long[] etimes = formattime(dateToMillis(endtime));

        byte[] skey = new byte[16];
        byte[] ekey = new byte[16];

//        Bytes.putBytes(skey, 0, pumpid, 0, 4);
//        Bytes.putLong(skey, 4, stimes[0]);
//        Bytes.putInt(skey, 12, 0);
//
//        Bytes.putBytes(ekey, 0, pumpid, 0, 4);
//        Bytes.putLong(ekey, 4, stimes[0] + 1);
//        Bytes.putInt(ekey, 12, Integer.MAX_VALUE);

        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.put(pumpid);
        bb.put(Bytes.toBytes(stimes[0]));
        bb.put(Bytes.toBytes(0));
        bb.rewind();
        bb.get(skey);


        bb = ByteBuffer.allocate(16);
        bb.put(pumpid);
        bb.put(Bytes.toBytes(etimes[1]+1));
        bb.put(Bytes.toBytes(Integer.MAX_VALUE));
        bb.rewind();
        bb.get(ekey);


        Scan scan = new Scan(skey, ekey);
//        Scan scan = new Scan();

        return scan;
    }


    /**
     * Fuzzy row setup filter.
     * @param tagids
     * @param datakey
     * @return
     */
    private FuzzyRowFilter setupFuzzyFilter(List<Integer> tagids, byte[] datakey) {


        List<Pair<byte[], byte[]>> pairs= new ArrayList<Pair<byte[], byte[]>>();

        for (int tagid : tagids) {
            byte[] rowkey = Arrays.copyOf(datakey, datakey.length);
            Bytes.putInt(rowkey, 12, tagid);
            pairs.add(new Pair(rowkey, mask));
        }

        FuzzyRowFilter frowFilter1 = new FuzzyRowFilter(pairs);

        return frowFilter1;
    }

    public static void main1(String args[]) {
        TagEventData tagEventData = new TagEventData();
        try {
//            tagEventData.getAllDataForAPump("pump1", "2014-10-16T18:25:12.0058824Z", "2014-10-21T21:15:12.0058824Z");
            tagEventData.getAllDataForAPump("pump1", "2014-10-16T19:15:12.0058824Z", "2014-10-16T19:45:12.019608Z");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public static void main(String args[]) throws IOException {
        getAllRowKeys();
    }

    public static List<String> getAllRowKeys() throws IOException {
        List<String> rowKeyList = new ArrayList<String>();
        String space = "        ";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss:SSS");
        Scan scan = new Scan();

        FilterList fl = new FilterList();
        // returns first instance of a row, then skip to next row
        fl.addFilter(new FirstKeyOnlyFilter());
        // only return the Key, don't return the value
        fl.addFilter(new KeyOnlyFilter());
        scan.setFilter(fl);

        HTableInterface tagtable = SingletonConnection.getInstance(TBL_TAG_EVENTS);
        ResultScanner scanner = tagtable.getScanner(scan);
        TagRowKey rowKey = new TagRowKey();
        MetaLookup metaLookup = new MetaLookup();
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            byte[] key = rr.getRow();
            long keys[] = rowKey.deconstructRowkey1(key);
            log.info("keys.length==" + keys.length);
            StringBuilder stringBuilder = new StringBuilder();
//            for(long aKeys: keys){
            for(int i=0; i<keys.length; i++){
                log.info("aKeys==" + keys[i]);

                if(i==0){
                    String pumpId = metaLookup.getPumpIdForPumpIdNo((int)keys[0]);
                    stringBuilder.append(pumpId).append(space);
                }
                else if(i==1) {
                    Date date = new Date(keys[i]);
                    stringBuilder.append(dateFormat.format(date)).append(space);
                }
                else if(i==2){
                    String tagId = metaLookup.getTagIdForTagIdNo((int)keys[2],(int)keys[0]);
                    stringBuilder.append(tagId);
                }
            }
            rowKeyList.add(stringBuilder.toString());
        }

        return rowKeyList;
    }
}
