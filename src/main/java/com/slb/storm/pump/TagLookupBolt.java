package com.slb.storm.pump;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.slb.common.util.CacheUtil;
import com.slb.common.util.CacheUtil2;
import com.slb.hbase.lookup.MetaLookup;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class TagLookupBolt implements IRichBolt
{
    private static final Logger LOG = Logger.getLogger(TagLookupBolt.class);
    private OutputCollector collector;
//    private CacheUtil cacheUtil;
    private MetaLookup metaLookup;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;
        try {
            this.metaLookup = new MetaLookup();
//            cacheUtil = CacheUtil.getInstance();
        } catch (IOException e) {
            LOG.error("Exception while initializing cacheUtil: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple)
    {
        LOG.info("START TagLookupBolt.execute()");
        String tagId = tuple.getStringByField(ESPScheme.TAG_ID);
        long eventTime = tuple.getLongByField(ESPScheme.EVENT_TIME);
        double value = tuple.getDoubleByField(ESPScheme.VALUE);
        double quality = tuple.getDoubleByField(ESPScheme.QUALITY);
        String comment = tuple.getStringByField(ESPScheme.COMMENT);

        Integer pumpId = null;
        try {
            pumpId = CacheUtil2.getTagToPumpLookupCache(tagId);
            if(pumpId==null){
                pumpId = metaLookup.getPumpIdforTag(tagId);
                CacheUtil2.putTagToPumpLookupCache(tagId, pumpId);
            }

            LOG.info("TagLookupBolt.execute(): Pump Id=" + pumpId + " for Tag Id=" + tagId);
            if(pumpId != 0) {
                LOG.debug("TagLookupBolt.execute(): Emitting tuple with tagId=" + tagId);
                collector.emit(tuple, new Values(pumpId, tagId, eventTime, value, quality, comment));
            } else {
                LOG.error("Incorrect or no pump id matching the tag. pumpId=" + pumpId + " for tagId=" + tagId + ".");
            }
        } catch (IOException e) {
            LOG.error("Exception while metadata lookup: " + e);
        }

        //acknowledge even if there is an error
        collector.ack(tuple);
        LOG.info("END TagLookupBolt.execute()");
    }



    @Override
    public void cleanup()
    {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
        new Fields(ESPScheme.PUMP_ID,
                ESPScheme.TAG_ID,
                ESPScheme.EVENT_TIME,
                ESPScheme.VALUE,
                ESPScheme.QUALITY,
                ESPScheme.COMMENT
                )
        );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
