package com.slb.common.util;

import com.slb.common.data.TagMetaData;
import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;


/**
 * Created by dpramodv on 12/9/14.
 */
public class CacheUtil2 {
    private static final Logger logger = Logger.getLogger(CacheUtil2.class);

    public static final int TAG_TO_PUMP_CACHE_SIZE = 14000;

    private static LRUMap _tagTagPumpLookupCache = new LRUMap(TAG_TO_PUMP_CACHE_SIZE);
    private static LRUMap _tagToTagMetaDataLookupCache = new LRUMap(TAG_TO_PUMP_CACHE_SIZE);

    public synchronized static void putTagToPumpLookupCache(String tag, Integer pump) {
        logger.error("Publishing to Lookup cache.." + tag);
        _tagTagPumpLookupCache.put(tag, pump);
    }

    public  static Integer getTagToPumpLookupCache(String key){
        return (Integer)_tagTagPumpLookupCache.get(key);
    }

    public synchronized static void putTagToTagMetaDataLookupCache(String tag, TagMetaData tagMetaData) {
        logger.error("Publishing to Index cache.." + tag);
        _tagToTagMetaDataLookupCache.put(tag, tagMetaData);
    }

    public static TagMetaData getTagToMetaDataLookupCache(String key) {
        return (TagMetaData)_tagToTagMetaDataLookupCache.get(key);
    }

    public static void main(String args[]){
        System.out.println("START CacheUtil2");
        String tagId="testKey";
        Integer pumpId = CacheUtil2.getTagToPumpLookupCache(tagId);
        if(pumpId == null){
            //Create a pump
            pumpId = 55;
            CacheUtil2.putTagToPumpLookupCache(tagId, pumpId);
        }

        Integer pumpId2 = CacheUtil2.getTagToPumpLookupCache(tagId);
        if (pumpId.intValue() == pumpId2.intValue()) {
            System.out.println("Pump id matches");
        } else {
            System.out.println("Pump id from cache does not match");
        }

        System.out.println("END CacheUtil2");
    }

}

