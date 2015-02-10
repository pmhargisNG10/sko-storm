package com.hortonworks.hbase.lookup;

import java.io.IOException;
import java.util.*;

import com.hortonworks.common.data.Pump;
import com.hortonworks.common.data.TagMetaData;
import com.hortonworks.hbase.connection.SingletonConnection;
import com.hortonworks.hbase.persist.MetaData;
import com.hortonworks.hbase.rowkeys.MetaRowKey;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class MetaLookup {
    private static final Logger LOG = Logger.getLogger(MetaLookup.class);
    private HTableInterface _tagMetaDataTbl = null;

    public MetaLookup() throws IOException{
        getTagMetaDataTbl();
    }

    /**
     *
     * @return
     * @throws IOException
     */
    private void getTagMetaDataTbl() throws IOException {
        _tagMetaDataTbl = SingletonConnection.getInstance(MetaData.TAG_META_DATA_TABLE);
    }

    /**
     * Return the pump id, tag id for the given tag name
     * @return
     * @throws IOException
     */
//    public TagMetaData getMetaDataforTag(String tagname) throws IOException {
//        LOG.info("START MetaLookup.getMetaDataforTag()");
//        LOG.info("****** tagId=" +tagname);
//        TagMetaData tagMetaData = null;
//
//        byte[] mrowkey = new MetaRowKey().constructRowKey(null, tagname.trim(), MetaRowKey.TAG);
//
//        //HBASE lookup for mRowkey
//        Get get = new Get(mrowkey);
//        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
//        Result rs = _tagMetaDataTbl.get(get);
//        if (rs != null) {
//            //TAG ID FOLLOWED BY PUMP ID
//            double hi = Bytes.toDouble(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.HI));
//            double low = Bytes.toDouble(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.LOW));
//            double hihi = Bytes.toInt(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.HIHI));
//            double lolo = Bytes.toInt(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.LOLO));
//            String uom = Bytes.toString(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.UOM));
//            String desc = Bytes.toString(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.DESC));
//
//            int pumpIdNo = Bytes.toInt(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO));
//            int tagIdNo = 0;
//            if(rs.containsColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO)) {
//                tagIdNo = Bytes.toInt(rs.getValue(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO));
//            } else {
//                LOG.error("########## tagIdNo is missing for tagId: " + tagname);
//            }
//
//            tagMetaData = new TagMetaData();
//            tagMetaData.setTagId(tagIdNo);
//            tagMetaData.setTag(tagname);
//            tagMetaData.setPumpId(pumpIdNo);
//            tagMetaData.setHighValue(hi);
//            tagMetaData.setLowValue(low);
//            tagMetaData.setHighHighValue(hihi);
//            tagMetaData.setLowLowValue(lolo);
//            tagMetaData.setUom(uom);
//            tagMetaData.setDesc(desc);
//
//        }
//        //todo: ??????
//        _tagMetaDataTbl.close();
//        LOG.info("END MetaLookup.getMetaDataforTag()");
//        return tagMetaData;
//
//    }

    /**
     * Get all tag ids for a given pump id
     * @param pumpId
     * @return
     */
    public List<TagMetaData> getAllTagMetaDataforPump(int pumpId) throws IOException {
        byte[] startRowKey = MetaRowKey.constructRowKey(Integer.toString(pumpId), Integer.toString(0), MetaRowKey.MAPPING);
//        byte[] endRowKey = MetaRowKey.constructRowKey(Integer.toString(pumpId), Integer.toString(Integer.MAX_VALUE), MetaRowKey.MAPPING);
        byte[] endRowKey = MetaRowKey.constructRowKey(Integer.toString(pumpId+1), Integer.toString(2), MetaRowKey.MAPPING);
        Scan scan = new Scan(startRowKey, endRowKey);
        scan.addColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID);

        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
        ResultScanner scanner = _tagMetaDataTbl.getScanner(scan);
        List<TagMetaData> tagMetaDataList = new ArrayList<TagMetaData>();
        TagMetaData tagMetaData = null;
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            String tagId = Bytes.toString(rr.getValue(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID));
            tagMetaData = getTagMetaDataforTag(tagId);
            tagMetaDataList.add(tagMetaData);
        }
        //todo: ????
        _tagMetaDataTbl.close();
        return tagMetaDataList;

    }

    /**
     * Get all tagId's from mapping
     * @param pumpId
     * @return
     * @throws IOException
     */
    public List<String> getTagsIdsForPump(int pumpId) throws IOException {
        byte[] startRowKey = MetaRowKey.constructRowKey(Integer.toString(pumpId), Integer.toString(0), MetaRowKey.MAPPING);
        byte[] endRowKey = MetaRowKey.constructRowKey(Integer.toString(pumpId), Integer.toString(Integer.MAX_VALUE), MetaRowKey.MAPPING);
//        byte[] endRowKey = MetaRowKey.constructRowKey(Integer.toString(pumpId+1), Integer.toString(2), MetaRowKey.MAPPING);
        Scan scan = new Scan(startRowKey, endRowKey);
        scan.addColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID); // Mapping does not have tagIdNo

        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
        ResultScanner scanner = _tagMetaDataTbl.getScanner(scan);
        List<String> stringList = new ArrayList<String>();
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            stringList.add(Bytes.toString(rr.getValue(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID)));
        }
        //todo: ????
        _tagMetaDataTbl.close();
        return stringList;

    }


    /**
     * Get tagidNo for a given tagname.
     * @param tagname
     * @return
     */
    public int getTagIdforTag(String tagname) throws IOException{
        byte[] tagRowKey = MetaRowKey.constructRowKey(null, tagname, MetaRowKey.TAG);
        Get get = new Get(tagRowKey);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO);
        int tagIdNo = 0;
        if(_tagMetaDataTbl == null) getTagMetaDataTbl();

        Result result = _tagMetaDataTbl.get(get);
        if(result.containsColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO)){
            tagIdNo = Bytes.toInt(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO));
        } else {
            System.out.println("Column: " + MetaData.TAG_ID_NO + " not found.");
        }
        _tagMetaDataTbl.close();
        return tagIdNo;
    }

    /**
     * Get tagidNo for a given tagname.
     * @param tagname
     * @return
     */
    public synchronized TagMetaData getTagMetaDataforTag(String tagname) throws IOException{
        TagMetaData tagMetaData = null;
        byte[] tagRowKey = MetaRowKey.constructRowKey(null, tagname, MetaRowKey.TAG);
        Get get = new Get(tagRowKey);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.HI);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.LOW);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.HIHI);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.LOLO);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.UOM);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_TYPE);

        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
        Result result = _tagMetaDataTbl.get(get);
        LOG.info("##### TagMetaData.getTagMetaDataforTag:" + tagname);
        if(result != null) {

            int tagIdNo = 0;
            LOG.info("Result: " + result);
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO)) {
                tagIdNo = Bytes.toInt(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.TAG_ID_NO));
            }
            int pumpIdNo =0;
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO)) {
                pumpIdNo = Bytes.toInt(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO));
            }
            double hi = 0.0;
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.HI)) {
                hi = Bytes.toDouble(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.HI));
            }
            double low = 0.0;
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.LOW)) {
                low = Bytes.toDouble(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.LOW));
            }
            double hihi = 0.0;
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.HIHI)) {
                hihi = Bytes.toDouble(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.HIHI));
            }
            double lolo = 0.0;
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.LOLO)) {
                lolo = Bytes.toDouble(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.LOLO));
            }

            String uom = null;
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.UOM)) {
                uom = Bytes.toString(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.UOM));
            }

            String tagType = null;
            if(result.containsNonEmptyColumn(MetaData.TAG_META_DATA_CF, MetaData.TAG_TYPE)) {
                tagType = Bytes.toString(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.TAG_TYPE));
            }

            tagMetaData = new TagMetaData();
            tagMetaData.setPumpId(pumpIdNo);
            tagMetaData.setTagId(tagIdNo);
            tagMetaData.setHighValue(hi);
            tagMetaData.setLowValue(low);
            tagMetaData.setHighHighValue(hihi);
            tagMetaData.setLowLowValue(lolo);
            tagMetaData.setUom(uom);
            tagMetaData.setTagType(tagType);

            tagMetaData.setTag(tagname);

            LOG.debug("#### TagMetaData=" + tagMetaData);

        } else {
            LOG.error("Empty Result for tag " + tagname);
        }
        _tagMetaDataTbl.close();
        return tagMetaData;
    }

    /**
     * Get pumpIdNo for a given tagname.
     * @param tagname
     * @return
     */
    public int getPumpIdforTag(String tagname) throws IOException {
        byte[] tagRowKey = MetaRowKey.constructRowKey(null, tagname, MetaRowKey.TAG);
        Get get = new Get(tagRowKey);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO);

        int pumpIdNo = 0;

        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
        Result result = _tagMetaDataTbl.get(get);
        if(result.containsColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO)){
            pumpIdNo = Bytes.toInt(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO));
        } else {
            System.out.println("Column: " + MetaData.PUMP_ID_NO + " not found.");
        }
        _tagMetaDataTbl.close();

        return pumpIdNo;
    }

    /**
     * Get pumpIdNo for a given tagname.
     * @param pumpname
     * @return
     */
    public int getPumpIdforPump(String pumpname) throws IOException {
        byte[] pumpRowKey = MetaRowKey.constructRowKey(pumpname, null, MetaRowKey.PUMP);
        Get get = new Get(pumpRowKey);
        get.addColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO);
        int pumpIdNo = 0;

        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
        Result result = _tagMetaDataTbl.get(get);
        if(result.containsColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO)){
            pumpIdNo = Bytes.toInt(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO));
        } else {
            System.out.println("Column: " + MetaData.PUMP_ID_NO + " not found.");
        }
        _tagMetaDataTbl.close();

        return pumpIdNo;
    }

    /**
     * Get all pumps
     * @return null if no pumps else a list of pumps
     */
    public List<Pump> getAllPumps() throws IOException {
        byte[] sKey = MetaRowKey.constructRowKey("", null, MetaRowKey.PUMP);
        byte[] eKey = MetaRowKey.constructRowKey(null, "", MetaRowKey.TAG);
        Scan scan = new Scan(sKey, eKey);
        scan.addColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO);
        scan.addColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_NAME);
        scan.addColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_TYPE);

        List<Pump> pumpList = new ArrayList<Pump>(30);

        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
        //iterate over scan, retrive pumpId from rowkey, pumpIdNo and later pumpName
        ResultScanner scanner = _tagMetaDataTbl.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            byte[] bytes = result.getRow();
            String pumpId = Bytes.toString(bytes, 2, (bytes.length - 2));
            int pumpIdNo = Bytes.toInt(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO));
            String pumpName = Bytes.toString(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_NAME));
            String pumpType = Bytes.toString(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_TYPE));

            Pump pump = new Pump();
            pump.setPumpIdNo(pumpIdNo);
            pump.setPumpId(pumpId);
            pump.setPumpName(pumpName);
            pump.setPumpType(pumpType);

            pumpList.add(pump);
        }
        return pumpList.isEmpty()?null:pumpList;
    }

   /**
     * Get pump for pumpid
     * @return null if no pumps else a list of pumps
     */
    public Pump getPumpforPump(String pumpId) throws IOException {
        byte[] pumpRowKey = MetaRowKey.constructRowKey(pumpId, null, MetaRowKey.PUMP);
        Get get = new Get(pumpRowKey);
        int pumpIdNo = 0;

        if(_tagMetaDataTbl == null) getTagMetaDataTbl();
        Result result = _tagMetaDataTbl.get(get);
        if(result.containsColumn(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO)){
            pumpIdNo = Bytes.toInt(result.getValue(MetaData.TAG_META_DATA_CF, MetaData.PUMP_ID_NO));
        }

        Pump pump = new Pump();
        pump.setPumpId(pumpId);
        pump.setPumpIdNo(pumpIdNo);

        _tagMetaDataTbl.close();

        return pump;
    }

    public String getPumpIdForPumpIdNo(int pumpIdNo) throws IOException {
        String pumpId = null;
        List<Pump> pumpList = getAllPumps();
        for(Pump aPump : pumpList){
            if(aPump.getPumpIdNo() == pumpIdNo){
                pumpId = aPump.getPumpId();
            }
        }
        return pumpId;
    }

    public String getTagIdForTagIdNo(int tagIdNo, int pumpIdNo) throws IOException {
        String tagId = null;
        List<TagMetaData> tagMetaDataList = getAllTagMetaDataforPump(pumpIdNo);
        for(TagMetaData tagMetaData: tagMetaDataList){
            if(tagMetaData.getTagId() == tagIdNo){
                tagId = tagMetaData.getTag();
            }
        }
        return tagId;
    }

    public static void main1(String... args) throws IOException {
        MetaLookup metaLookup = new MetaLookup();
        try {
            String tagname = null;
            if(args.length > 0) {
                TagMetaData tagMetaData = metaLookup.getTagMetaDataforTag(args[0]);
                System.out.println("tagMetaData.getTagId() == " + tagMetaData.getTagId());
                System.out.println("tagMetaData.getTag() == " + tagMetaData.getTag());
            }


//            TagMetaData tagMetaData = metaLookup.getMetaDataforTag("Accel_Pump_head_90");
//            TagMetaData tagMetaData = metaLookup.getMetaDataforTag("033b35cd2ee341be9993e71c35349904");
            TagMetaData tagMetaData = metaLookup.getTagMetaDataforTag("630e7ed289b74d7c81a3914f66ea108d");
            System.out.println("tagIdNo=" + tagMetaData.getTagId());
            System.out.println("pumpIdNo=" + tagMetaData.getPumpId());

            System.out.println("Get all PUMPS");
            List<Pump> pumpList = metaLookup.getAllPumps();
            for (Pump pump: pumpList){
                System.out.println(pump);
            }

            System.out.println("TAG ID LIST");
            List<String> tagIdList = metaLookup.getTagsIdsForPump(1);

            System.out.println("TAG META DATA LIST");
            List<TagMetaData> tagMetaDataList = metaLookup.getAllTagMetaDataforPump(2);

//            tagMetaData = metaLookup.getTagMetaDataforTag("Accel_Pump_head_90");
            tagMetaData = metaLookup.getTagMetaDataforTag("033b35cd2ee341be9993e71c35349904");
            System.out.println("TAG Data " + tagMetaData);

            int pumpId = metaLookup.getPumpIdforTag("f1d3873f14eb4734abb858b8dc6a232c");
            System.out.println("PUMP ID=" + pumpId);

            String tagIds[] = {"a24f0a956be24cccb2046119e2ea4813","cdf4858847ff4b219b101f5bd1af9442",
                    "9a38b802ec0c4f2091e2dfe14fcd7889", "536ce6e4b51e47bfa38fa1e02569362e",
                    "e9ed860a799e40d1948fdf697d841495"};
            for(String atagId:tagIds){
                TagMetaData metaDataforTag = metaLookup.getTagMetaDataforTag(atagId);
                System.out.println(metaDataforTag);
            }
//            List<Integer> tagIds = metaLookup.lookupTagsForPump(tagMetaData.getPumpId());
//
//            for(int a : tagIds) {
//                System.out.println("Tag id=" + a);
//            }
//
//            // Get tagIdNo for given tagnam X1 Phs Volts Mtr
//            int tagIdNo = metaLookup.getTagIdforTag("X1 Phs Volts Mtr");
//            System.out.println("** tagIdNo == " + tagIdNo);
//
//            // get pump id for pump
//            int pumpIdNo = metaLookup.getPumpIdforPump("pump3");
//            System.out.println("** pumpIdNo == " + pumpIdNo);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws IOException {
        MetaLookup metaLookup = new MetaLookup();
        System.out.println("TAG META DATA LIST");
        List<TagMetaData> tagMetaDataList = metaLookup.getAllTagMetaDataforPump(2);
        for(TagMetaData tagMetaData: tagMetaDataList){
            System.out.println(tagMetaData);
        }
    }

}