package com.slb.storm.pump;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.slb.hbase.lookup.MetaLookup;
import org.apache.log4j.Logger;

import java.util.Map;

public class DtsEventBolt implements IRichBolt
{
    private static final Logger LOG = Logger.getLogger(DtsEventBolt.class);
    private OutputCollector collector;
//    private CacheUtil cacheUtil;
    private MetaLookup metaLookup;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;
//        try {
////            this.metaLookup = new MetaLookup();
////            cacheUtil = CacheUtil.getInstance();
//        } catch (IOException e) {
//            LOG.error("Exception while initializing cacheUtil: " + e);
//            e.printStackTrace();
//        }
    }

    @Override
    public void execute(Tuple tuple)
    {
        LOG.info("START SkoStormDemoBolt.execute()");
        long traceTime = tuple.getLongByField(ESPScheme.TRACE_KEY);
        double depth = tuple.getDoubleByField(ESPScheme.DEPTH_KEY);
        double temperature = tuple.getDoubleByField(ESPScheme.TEMP_KEY);

//        Integer pumpId = null;
//        try {
//            pumpId = CacheUtil2.getTagToPumpLookupCache(tagId);
//            if(pumpId==null){
//                pumpId = metaLookup.getPumpIdforTag(tagId);
//                CacheUtil2.putTagToPumpLookupCache(tagId, pumpId);
//            }

            LOG.info("SkoStormDemoBolt.execute(): TraceTime=" + traceTime);
            if(traceTime != 0) {
                LOG.debug("SkoStormDemoBolt.execute(): Emitting tuple for TraceTime" + traceTime);
                collector.emit(tuple, new Values(traceTime, depth, temperature));
            } else {
                LOG.error("Bad stuff");
            }
//        } catch (IOException e) {
//            LOG.error("Exception while metadata lookup: " + e);
//        }

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
        new Fields(ESPScheme.TRACE_KEY,
                ESPScheme.DEPTH_KEY,
                ESPScheme.TEMP_KEY
                )
        );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
