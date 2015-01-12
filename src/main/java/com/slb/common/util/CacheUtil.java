package com.slb.common.util;

import com.slb.common.data.TagMetaData;
import com.slb.hbase.lookup.MetaLookup;
import org.apache.commons.collections.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by dpramodv on 12/2/14.
 */
public class CacheUtil {
    private static final Logger logger = LoggerFactory.getLogger(CacheUtil.class);
    public static final int TAG_TO_PUMP_CACHE_SIZE = 200000;  // as we have 10 workers and 14000 tags

    private static CacheUtil cacheUtil;

    private Map<String, Integer> _tagIdToPumpIdLookupCache;
    private Map<String, TagMetaData> _tagIdToTagMetaDataLookupCache;

    private Object obj = new Object();
    private static Object _sobj = new Object();

    private MetaLookup metaLookup;

    private CacheUtil() throws IOException {
        metaLookup = new MetaLookup();
        synchronized(obj) {
            if (_tagIdToPumpIdLookupCache == null) {
                _tagIdToPumpIdLookupCache = new LRUMap(TAG_TO_PUMP_CACHE_SIZE);
            }

            if (_tagIdToTagMetaDataLookupCache == null) {
                _tagIdToTagMetaDataLookupCache = new LRUMap(TAG_TO_PUMP_CACHE_SIZE);
            }

        }
    }

    /**
     *
     * @param tagId
     * @param tagMetaData
     */
    private synchronized void putTagLookupCache(String tagId, TagMetaData tagMetaData) {
        logger.info("Publishing TagMetaData to Lookup cache:" + tagId);
        _tagIdToTagMetaDataLookupCache.put(tagId, tagMetaData);
    }

    /**
     *
     * @param tagId
     * @return
     * @throws IOException
     */
    public TagMetaData lookupTagCache(String tagId) throws IOException {
        TagMetaData tagMetaData = _tagIdToTagMetaDataLookupCache.get(tagId);
        if(tagMetaData != null){
            logger.info("Got TagMetaData from cache!!!!!");
            return tagMetaData;
        }

        //get from hbase and put
        logger.info("Getting TagMetaData from hbase #######");
        tagMetaData = metaLookup.getTagMetaDataforTag(tagId);
        putTagLookupCache(tagId, tagMetaData);

        return tagMetaData;
    }

    /**
     *
     * @param tagId
     * @param
     */
    private synchronized void putTagIdToPumpIdLookupCache(String tagId, int pumpId) {
        logger.info("Publishing pumpId=" + pumpId + " to Lookup cache for tagId=" + tagId);
        _tagIdToPumpIdLookupCache.put(tagId, pumpId);
    }

    /**
     *
     * @param tagId
     * @return
     * @throws IOException
     */
    public int lookupTagIdToPumpIdCache(String tagId) throws IOException {
        Integer pumpId = _tagIdToPumpIdLookupCache.get(tagId);
        if(pumpId != null && pumpId.intValue() > 0){
            logger.info("Got pumpId from cache!!!!!");
            return pumpId;
        }

        //get from hbase and put
        logger.info("Getting pumpId from hbase #######");
        pumpId = metaLookup.getPumpIdforTag(tagId);
        putTagIdToPumpIdLookupCache(tagId, pumpId);

        return pumpId;
    }


    public static CacheUtil getInstance() throws IOException {
        if(cacheUtil == null) {
            synchronized (_sobj) {
                cacheUtil = new CacheUtil();
            }
        }
        return cacheUtil;
    }

    public static void main(String args[]) throws IOException {
        String[] tagids = {
        "a4dbe8683ddb420e80fe6ccbf69c6bf5",
                "f1d3873f14eb4734abb858b8dc6a232c",
        "1a6c8fe9c20e4e02bc0aaad11671a27b",
        "630e7ed289b74d7c81a3914f66ea108d",
        "1b84c374f70e4177a50ab44916284448",
        "98a2908a16574950a3194046d999a77d",
        "08635f622c0740c99c6d8e8cc9279745",
                "ecb81f0f246b4c749ada45c20bbfde7b",
                "a24f0a956be24cccb2046119e2ea4813",
                "cdf4858847ff4b219b101f5bd1af9442",
                "9a38b802ec0c4f2091e2dfe14fcd7889",
                "536ce6e4b51e47bfa38fa1e02569362e",
                "e9ed860a799e40d1948fdf697d841495"
        };

        Random randomGenerator = new Random();
        CacheUtil cacheUtil = CacheUtil.getInstance();
        System.out.println("START cacheUtil.lookupTagCache(tagid)");
        int i =0;
        for(i=0; i<=100; i++) {
            String tagid = tagids[randomGenerator.nextInt(tagids.length - 1)];
            TagMetaData metaData = cacheUtil.lookupTagCache(tagid);
            System.out.println(metaData);
        }
        System.out.println("END cacheUtil.lookupTagCache(tagid)");
        System.out.println("#####################################");
        System.out.println("#####################################");
        System.out.println("START cacheUtil.lookupTagIdToPumpIdCache(tagid)");

        for(i=0; i<=100; i++) {
            String tagid = tagids[randomGenerator.nextInt(tagids.length - 1)];
            int pumpId = cacheUtil.lookupTagIdToPumpIdCache(tagid);
            System.out.println("PumpId=" + pumpId);
        }
        System.out.println("END cacheUtil.lookupTagIdToPumpIdCache(tagid)");

    }



}
