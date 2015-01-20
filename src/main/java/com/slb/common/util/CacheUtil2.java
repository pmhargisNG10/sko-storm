package com.slb.common.util;

import org.apache.commons.collections.map.LRUMap;
import org.apache.log4j.Logger;

import java.io.IOException;


public class CacheUtil2 {
    private static final Logger logger = Logger.getLogger(CacheUtil2.class);

    public static final int MIN_TRACE_CACHE_SIZE = 10000;
    public static final int MAX_TRACE_CACHE_SIZE = 10000;

    public static final String TEST_TRACE_ID = "2013-05-05T17:24:45.000Z";
    public static final double TEST_MIN_VALUE = 2.1415927;
    public static final double TEST_MAX_VALUE = 3.1415927;

    private static LRUMap _minTraceLookupCache = null;
    private static LRUMap _maxTraceLookupCache = null;

    private static Object LOCK = new Object();
    private static CacheUtil2 singleton = null;

    private CacheUtil2() throws IOException {
        synchronized(LOCK) {
            if (_minTraceLookupCache == null) {
                _minTraceLookupCache = new LRUMap(MIN_TRACE_CACHE_SIZE);
            }

            if (_maxTraceLookupCache == null) {
                _maxTraceLookupCache = new LRUMap(MAX_TRACE_CACHE_SIZE);
            }
        }
    }

    public static CacheUtil2 getInstance() throws IOException {
        if(singleton == null) {
            synchronized (LOCK) {
                singleton = new CacheUtil2();
            }
        }
        return singleton;
    }

    public synchronized static void putMaxTraceCacheValue(String traceId, double value) {
        logger.error("Publishing to Index cache.." + traceId);
        _maxTraceLookupCache.put(traceId, Double.valueOf(value));
    }

    public static Double getMaxTraceCacheValue(String tracedId) {
        return (Double)_maxTraceLookupCache.get(tracedId);
    }

    public synchronized static void putMinTraceCacheValue(String traceId, double value) {
        logger.error("Publishing to Index cache.." + traceId);
        _minTraceLookupCache.put(traceId, Double.valueOf(value));
    }

    public static Double getMinTraceCacheValue(String traceId) {
        return (Double)_minTraceLookupCache.get(traceId);
    }


    private static void testMinValueCache(String testTraceId) {
        Double minValue = new Double(TEST_MIN_VALUE);
        CacheUtil2.putMinTraceCacheValue(testTraceId, minValue);

        Double minValue2 = CacheUtil2.getMinTraceCacheValue(testTraceId);
        if (minValue.doubleValue() == minValue2.doubleValue()) {
            System.out.println("Success - Min values from cache match");
        } else {
            System.out.println("Fail - Min values from cache do not match");
        }
    }

    private static void testMaxValueCache(String testTraceId) {
        Double maxValue = new Double(TEST_MAX_VALUE);
        CacheUtil2.putMaxTraceCacheValue(testTraceId, maxValue);

        Double maxValue2 = CacheUtil2.getMaxTraceCacheValue(testTraceId);
        if (maxValue.doubleValue() == maxValue2.doubleValue()) {
            System.out.println("Success - Max values from cache match");
        } else {
            System.out.println("Fail - Max values from cache do not match");
        }
    }

    public static void main(String args[]){
        System.out.println("START CacheUtil2");
        try {
            CacheUtil2 cache = CacheUtil2.getInstance();
        } catch (IOException ex) {
            System.exit(-1);
        }
        String testTraceId = TEST_TRACE_ID;
        testMinValueCache(testTraceId);
        testMaxValueCache(testTraceId);

        System.out.println("END CacheUtil2");
    }

}

