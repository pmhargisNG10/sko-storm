package com.slb.hbase.persist;

import com.slb.common.data.Pump;
import com.slb.common.data.TagMetaData;
import com.slb.hbase.connection.SingletonConnection;
import com.slb.hbase.rowkeys.MetaRowKey;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dpramodv on 11/29/14.
 */
public class MetaData {
    private static final Logger LOG = Logger.getLogger(MetaData.class);
//    public static final String TAG_META_DATA_TABLE = "tag_meta_data";
    public static final String TAG_META_DATA_TABLE = "t_tag_meta_data";

    public static final byte[] PUMP_ROWKEY = Bytes.toBytes("pump_seq");
    public static final byte[] PUMP_CQ = Bytes.toBytes("pc");

    public static final byte[] TAG_ROWKEY = Bytes.toBytes("tag_seq");
    public static final byte[] TAG_CQ = Bytes.toBytes("tc");

    public static final byte[] SEQ_CF = Bytes.toBytes("s");
    public static final byte[] TAG_META_DATA_CF = Bytes.toBytes("m");

    public static final byte[] HI = Bytes.toBytes("hi");
    public static final byte[] LOW = Bytes.toBytes("low");
    public static final byte[] HIHI = Bytes.toBytes("hihi");
    public static final byte[] LOLO = Bytes.toBytes("lolo");
    public static final byte[] UOM = Bytes.toBytes("uom");
    public static final byte[] DESC = Bytes.toBytes("desc");
    public static final byte[] TAG_ID_NO = Bytes.toBytes("tagIdNo");
    public static final byte[] TAG_ID = Bytes.toBytes("tagId");

    public static final byte[] TAG_NAME = Bytes.toBytes("tagName");
    public static final byte[] TAG_TYPE = Bytes.toBytes("tagType");

    public static final byte[] PUMP_ID_NO = Bytes.toBytes("pumpIdNo");
    public static final byte[] PUMP_ID = Bytes.toBytes("pumpId");
    public static final byte[] PUMP_NAME = Bytes.toBytes("pumpName");
    public static final byte[] PUMP_TYPE = Bytes.toBytes("pumpType");

    HTableInterface tableInterface;
    public MetaData(){
        try {
            tableInterface = SingletonConnection.getInstance(TAG_META_DATA_TABLE);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e);
        }
    }


    /**
     * Generate new pump id
     * @return
     */
    private int getNextPumpId(){
        long pumpId = 0l;
        try {
            pumpId = tableInterface.incrementColumnValue(PUMP_ROWKEY, SEQ_CF, PUMP_CQ, 1);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e);
        }
        return (int)pumpId;
    }

    /**
     * Generate new tag id
     * @return
     */
    private int getNextTagId(){
        long tagId = 0l;
        try {
            tagId = tableInterface.incrementColumnValue(TAG_ROWKEY, SEQ_CF, TAG_CQ, 1);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error(e);
        }
        return (int)tagId;
    }



    public static void main(String... args) {
        System.out.println("START TagMetaData");
//        long pumpId = getNextPumpId();
//        long tagId = getNextTagId();
//        System.out.println("Pump id = " + pumpId);
//        System.out.println("Tag id = " + tagId);

        //Create pump
        String pumpId = "pump1";
        String pumpType = "pumpTypeV";
        String pumpName = "pumpNameV";
        Pump pump = new Pump();
        pump.setPumpId(pumpId);
        pump.setPumpName(pumpName);
        pump.setPumpType(pumpType);

        //Create Tag
        String tagId = "TAG_ID_XXXX-4444-5555";
        String tagName = "TAG_NAME VALUE";
        String tagTyoe = "TAG_TYPE VALUE";
        double hi = 5.0;
        double lo = -4.5;
        double hihi = 7.0;
        double lolo = -6;
        String uom = "F";
        String desc = "Accel_Pump_head_90 desc";

        TagMetaData tagMetaData = new TagMetaData();
        MetaData metaData = new MetaData();

        int pumpIdNo = metaData.getPumpId(pump);
        System.out.println("** pump id = " + pumpIdNo);

        tagMetaData.setPumpId(pumpIdNo);
        tagMetaData.setPump(pumpId);
        tagMetaData.setTag(tagId);
        tagMetaData.setTagType(tagTyoe);
        tagMetaData.setTagName(tagName);
        tagMetaData.setHighValue(hi);
        tagMetaData.setLowValue(lo);
        tagMetaData.setHighHighValue(hihi);
        tagMetaData.setLowLowValue(lolo);
        tagMetaData.setUom(uom);
        tagMetaData.setDesc(desc);

        int tagIdNo = metaData.getTagId(tagMetaData);
        System.out.println("** tag id = " + tagIdNo);

        metaData.createMapping(pumpIdNo, pumpId, tagIdNo, tagId);


        System.out.println("END TagMetaData");
    }

    /**
     * get existing pump id or create new pumpid.
     * @param pump
     * @return
     */
    public int getPumpId(Pump pump) {
        String pumpId = pump.getPumpId();
        byte[] pumpRowKey = MetaRowKey.constructRowKey(pumpId, null, MetaRowKey.PUMP);
        Get get = new Get(pumpRowKey);
        get.addColumn(TAG_META_DATA_CF, PUMP_ID_NO);
        int pumpIdNo = 0;
        HTableInterface table = null;
        try {
            table = SingletonConnection.getInstance(TAG_META_DATA_TABLE);
            Result result = table.get(get);
            if(result != null){
                //If pump exists return pump id
                if(result.containsColumn(TAG_META_DATA_CF, PUMP_ID_NO)) {
                    byte[] pumpIdBytes = result.getValue(TAG_META_DATA_CF, PUMP_ID_NO);
                    pumpIdNo = Bytes.toInt(pumpIdBytes);
                } else {
                    //add new pump and return the pumpId
                    Put put = new Put(pumpRowKey);
                    pumpIdNo = getNextPumpId();
                    put.add(TAG_META_DATA_CF, PUMP_ID_NO, Bytes.toBytes(pumpIdNo));
                    put.add(TAG_META_DATA_CF, PUMP_NAME, Bytes.toBytes(pump.getPumpName()));
                    put.add(TAG_META_DATA_CF, PUMP_TYPE, Bytes.toBytes(pump.getPumpType()));
                    table.put(put);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return pumpIdNo;
    }

    /**
     * Get tag id for existing tag or generate a new tagid
     * @param tagMetaData
     * @return
     */
    public int getTagId(TagMetaData tagMetaData) {
        String tagId = tagMetaData.getTag();
        byte[] tagRowKey = MetaRowKey.constructRowKey(null, tagId, MetaRowKey.TAG);
        Get get = new Get(tagRowKey);
        get.addColumn(TAG_META_DATA_CF, TAG_ID_NO);
        int tagIdNo = 0;
        HTableInterface table = null;
        try {
            table = SingletonConnection.getInstance(TAG_META_DATA_TABLE);
            Result result = table.get(get);
            if(result != null){
                //If tag exists so return tag id
                if(result.containsColumn(TAG_META_DATA_CF, TAG_ID_NO)) {
                    byte[] tagIdBytes = result.getValue(TAG_META_DATA_CF, TAG_ID_NO);
                    tagIdNo = Bytes.toInt(tagIdBytes);
                } else {
                    //add new tag and return tagId
                    Put put = new Put(tagRowKey);
                    tagIdNo = getNextTagId();
                    double hi = tagMetaData.getHighValue();
                    double low = tagMetaData.getLowValue();
                    double hihi = tagMetaData.getHighHighValue();
                    double lolo = tagMetaData.getLowLowValue();
                    String uom = tagMetaData.getUom();
                    String desc = tagMetaData.getDesc();
                    int pumpIdNo = tagMetaData.getPumpId();
                    String tagName = tagMetaData.getTagName();
                    String tagType = tagMetaData.getTagType();

                    put.add(TAG_META_DATA_CF, HI, Bytes.toBytes(hi));

                    put.add(TAG_META_DATA_CF, LOW, Bytes.toBytes(low));

                    put.add(TAG_META_DATA_CF, HIHI, Bytes.toBytes(hihi));

                    put.add(TAG_META_DATA_CF, LOLO, Bytes.toBytes(lolo));

                    put.add(TAG_META_DATA_CF, UOM, Bytes.toBytes(uom));

                    put.add(TAG_META_DATA_CF, DESC, Bytes.toBytes(desc));

                    put.add(TAG_META_DATA_CF, TAG_ID_NO, Bytes.toBytes(tagIdNo));

                    put.add(TAG_META_DATA_CF, PUMP_ID_NO, Bytes.toBytes(pumpIdNo));

                    put.add(TAG_META_DATA_CF, TAG_TYPE, Bytes.toBytes(tagType));

                    put.add(TAG_META_DATA_CF, TAG_NAME, Bytes.toBytes(tagName));

                    table.put(put);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return tagIdNo;
    }

    /**
     *
     * @param pumpIdNo
     * @param tagIdNo
     */
    public void createMapping(int pumpIdNo, String pumpname, int tagIdNo, String tagname) {
        HTableInterface table = null;
        try {
            table = SingletonConnection.getInstance(TAG_META_DATA_TABLE);

            byte[] mappingRowKey = MetaRowKey.constructRowKey(Integer.toString(pumpIdNo),
                    Integer.toString(tagIdNo), MetaRowKey.MAPPING);
            Put put = new Put(mappingRowKey);
            put.add(TAG_META_DATA_CF, PUMP_ID, Bytes.toBytes(pumpname));
            put.add(TAG_META_DATA_CF, TAG_ID, Bytes.toBytes(tagname));

            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }



}
