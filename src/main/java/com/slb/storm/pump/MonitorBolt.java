package com.slb.storm.pump;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.slb.common.data.AlertTypeEnum;
import com.slb.common.data.TagMetaData;
import com.slb.common.util.CacheUtil;
import com.slb.common.util.CacheUtil2;
import com.slb.hbase.connection.SingletonConnection;
import com.slb.hbase.lookup.MetaLookup;
import com.slb.hbase.persist.TagAlert;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class MonitorBolt implements IRichBolt
{
    private static final Logger LOG = Logger.getLogger(MonitorBolt.class);

    //TABLE
    private static final String TAG_ALERTS_TABLE_NAME =  "t_tag_alerts";

    private OutputCollector collector;
    private HTableInterface tagAlertsTable;
    private TagAlert tagAlert;
    private MetaLookup metaLookup;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector) 
    {
        this.collector = collector;
        try
        {
            this.tagAlertsTable = SingletonConnection.getInstance(TAG_ALERTS_TABLE_NAME);
            this.tagAlertsTable.setAutoFlushTo(true);
            this.tagAlert = new TagAlert();
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
        LOG.info("START MonitorBolt.execute()");
        //tag_id (string), pump_id(string), alert type=(alarm|error string), message(string)

        String tagId = tuple.getStringByField(ESPScheme.TAG_ID);
        long eventTime = tuple.getLongByField(ESPScheme.EVENT_TIME);
        double value = tuple.getDoubleByField(ESPScheme.VALUE);
        double quality = tuple.getDoubleByField(ESPScheme.QUALITY);
        String comment = tuple.getStringByField(ESPScheme.COMMENT);
        int pumpIdNo = tuple.getIntegerByField(ESPScheme.PUMP_ID);

        //validate event for alert
        validateAndLogAlert(tagId, pumpIdNo, eventTime, value, quality, comment);

        collector.emit(tuple, new Values(pumpIdNo, tagId, eventTime, value, quality, comment));
        //acknowledge even if there is an error
        collector.ack(tuple);
        LOG.info("END MonitorBolt.execute()");
    }

    /**
     * Validate tag
     * @param tagId
     * @param pumpIdNo
     * @param eventTime
     * @param value
     * @param quality
     * @param comment
     * @return
     */
    public boolean validateAndLogAlert(String tagId, int pumpIdNo, long eventTime, double value, double quality, String comment) {
        LOG.debug("START MonitorBolt.validateAndLogAlert()");
        boolean alert = false;
        try {
            LOG.info("Tag Lookup cache:" + tagId);
            TagMetaData tagMetaData = CacheUtil2.getTagToMetaDataLookupCache(tagId);
            if(tagMetaData == null){
                tagMetaData = metaLookup.getTagMetaDataforTag(tagId);
                CacheUtil2.putTagToTagMetaDataLookupCache(tagId, tagMetaData);
            }
            //Check for alarms (when value is above or below high and low)
            if(tagMetaData != null) {
                if (value < tagMetaData.getLowValue() ||
                        value > tagMetaData.getHighValue()) {
                    alert = true;
                        LOG.debug("### Alert reported ###");
                    //Check for error (when value is above and below hihi and lolo.
                    if (value < tagMetaData.getLowLowValue() ||
                            value > tagMetaData.getHighHighValue()) {
                        //Error=Yes log error.
                        logAlert(tagId, pumpIdNo, eventTime, value, quality, comment, AlertTypeEnum.ERROR);
                    } else {
                        //Warning=Yes, log warning.
                        logAlert(tagId, pumpIdNo, eventTime, value, quality, comment, AlertTypeEnum.WARNING);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Error while logging alert: " + e);
            e.printStackTrace();
        }
        LOG.debug("END MonitorBolt.validateAndLogAlert()");

        return alert;
    }

    /**
     *
     * @param tagId
     * @param pumpIdNo
     * @param eventTime
     * @param value
     * @param quality
     * @param comment
     * @param alertTypeEnum
     * @throws IOException
     */
    private void logAlert(String tagId, int pumpIdNo, long eventTime, double value, double quality, String comment, AlertTypeEnum alertTypeEnum) throws IOException {
        LOG.info("START MonitorBolt.logAlert()");
        LOG.info("##### Alert reported: type:" + alertTypeEnum.getType());
        try {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("This is a test alert of type: " + alertTypeEnum.getType())
                    .append(". eventTime=" + eventTime)
                    .append("|value=" + value)
                    .append("|quality=" + quality)
                    .append("|comment=" + comment);

            if(tagAlert==null){
                tagAlert = new TagAlert();
            }
            tagAlert.put(tagId, pumpIdNo, alertTypeEnum, stringBuilder.toString(), tagAlertsTable);
        } catch (IOException e) {
            LOG.error("Error inserting event into HBase table[" + TAG_ALERTS_TABLE_NAME + "]", e);
            throw e;
        }
        LOG.info("END MonitorBolt.logAlert()");

    }



    @Override
    public void cleanup()
    {
        try
        {
            tagAlertsTable.flushCommits();
            tagAlertsTable.close();
        }
        catch (Exception  e)
        {
            LOG.error("Error closing tables", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(ESPScheme.PUMP_ID, ESPScheme.TAG_ID, ESPScheme.EVENT_TIME,
                ESPScheme.VALUE, ESPScheme.QUALITY, ESPScheme.COMMENT));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
