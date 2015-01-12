package com.slb.storm.pump;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.slb.hbase.connection.SingletonConnection;
import com.slb.hbase.lookup.MetaLookup;
import com.slb.hbase.persist.TagPersist;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.Random;

public class TagHBaseBolt implements IRichBolt
{
    private static final Logger LOG = Logger.getLogger(TagHBaseBolt.class);

    //TABLE
    private static final String TAG_EVENTS_TABLE_NAME =  "t_tag_events";


    private OutputCollector collector;
    private HTableInterface tagEventsTable;
    private TagPersist tagPersist;
    private MetaLookup metaLookup;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;
        try
        {
            this.tagEventsTable = SingletonConnection.getInstance(TAG_EVENTS_TABLE_NAME);
            this.tagPersist = new TagPersist();
            this.metaLookup = new MetaLookup();

        }
        catch (Exception e)
        {
            String errMsg = "Error retrievinging connection and access to HBase Tables";
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void execute(Tuple tuple)
    {
        LOG.info("START TagHBaseBolt.execute()");
        System.out.println("START TagHBaseBolt.execute()");
        LOG.info("Tuple: " +  tuple.toString());

        String tagId = tuple.getStringByField(ESPScheme.TAG_ID);
        long eventTime = tuple.getLongByField(ESPScheme.EVENT_TIME);
        double value = tuple.getDoubleByField(ESPScheme.VALUE);
        double quality = tuple.getDoubleByField(ESPScheme.QUALITY);
        String comment = tuple.getStringByField(ESPScheme.COMMENT);
        int pumpId = tuple.getIntegerByField(ESPScheme.PUMP_ID);
        //TODO: Get tag_id_no from LRUMap
        int tagIdNo = 0;
        try {
            tagIdNo = metaLookup.getTagIdforTag(tagId);
            LOG.info("TagHBaseBolt.execute(): Pump Id=" + pumpId + " for Tag Id=" + tagId);
            tagPersist.writeTagValues(tagIdNo, pumpId, eventTime, value, quality, comment);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Error inserting event or looking up meta data:", e);
        } catch (ParseException e) {
            LOG.error("Error inserting event into HBase table["+ TAG_EVENTS_TABLE_NAME +"]", e);
        }

        collector.emit(tuple, new Values(pumpId, tagId, eventTime, value, quality, comment));
        //acknowledge even if there is an error
        collector.ack(tuple);
        LOG.info("END TagHBaseBolt.execute()");
        System.out.println("END TagHBaseBolt.execute()");
    }

    @Override
    public void cleanup()
    {
        try
        {
            tagEventsTable.close();
        }
        catch (Exception  e)
        {
            LOG.error("Error closing connections", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ESPScheme.PUMP_ID, ESPScheme.TAG_ID, ESPScheme.EVENT_TIME,
                ESPScheme.VALUE, ESPScheme.QUALITY, ESPScheme.COMMENT));
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }

}
