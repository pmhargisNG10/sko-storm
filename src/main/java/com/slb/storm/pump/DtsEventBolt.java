package com.slb.storm.pump;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.slb.common.util.CacheUtil2;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class DtsEventBolt implements IRichBolt
{
    private static final Logger LOG = Logger.getLogger(DtsEventBolt.class);
    private OutputCollector collector;
    private CacheUtil2 cacheUtil;
    //private MetaLookup metaLookup;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;
        try {
            cacheUtil = CacheUtil2.getInstance();
        } catch (IOException e) {
            LOG.error("Exception while initializing cacheUtil: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple)
    {
        LOG.info("START SkoStormDemoBolt.execute()");
        long traceTime = tuple.getLongByField(ESPScheme.TRACE_KEY);
        double depth = tuple.getDoubleByField(ESPScheme.DEPTH_KEY);
        double temperature = tuple.getDoubleByField(ESPScheme.TEMP_KEY);

        String traceId = new DateTime(new Date(traceTime)).toString();
//        try {
            Double minValue = CacheUtil2.getMinTraceCacheValue(traceId);
            if (minValue == null) {
                CacheUtil2.putMinTraceCacheValue(traceId, Double.valueOf(temperature));
            }

            LOG.info("SkoStormDemoBolt.execute(): TraceTime=" + traceTime);
            if(traceTime != 0) {
                LOG.debug("SkoStormDemoBolt.execute(): Emitting tuple for TraceTime" + traceTime);
                collector.emit(tuple, new Values(traceTime, depth, temperature));
            } else {
                LOG.error("Bad stuff happened...");
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
            new Fields(ESPScheme.TRACE_KEY, ESPScheme.DEPTH_KEY, ESPScheme.TEMP_KEY));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
