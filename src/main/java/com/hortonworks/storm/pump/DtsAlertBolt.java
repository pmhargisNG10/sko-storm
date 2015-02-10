package com.hortonworks.storm.pump;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.Map;

public class DtsAlertBolt implements IRichBolt
{
    private static final Logger LOG = Logger.getLogger(DtsAlertBolt.class);

    //TABLE
    //private static final String TAG_ALERTS_TABLE_NAME =  "t_tag_alerts";
    private static final double ALERT_LEVEL = 330.0;
    public static final String DTS_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private OutputCollector collector;
    private AlertMail alertMail;


    @Override
    public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector)
    {
        this.collector = collector;
        this.alertMail = new AlertMail();
    }

    @Override
    public void execute(Tuple tuple)
    {
        LOG.info("START AlertBolt.execute()");
        //tag_id (string), pump_id(string), alert type=(alarm|error string), message(string)

        long traceTime = tuple.getLongByField(ESPScheme.TRACE_KEY);
        double depth = tuple.getDoubleByField(ESPScheme.DEPTH_KEY);
        double temperature = tuple.getDoubleByField(ESPScheme.TEMP_KEY);

        //validate event for alert
        boolean alert = validateAndLogAlert(traceTime, depth, temperature);

        collector.emit(tuple, new Values(traceTime, depth, temperature));

        //acknowledge even if there is an error
        collector.ack(tuple);
        LOG.info("END AlertBolt.execute()");
    }


    public boolean validateAndLogAlert(long traceTime, double depth, double temp) {
        LOG.debug("START AlertBolt.validateAndLogAlert()");
        boolean alert = false;
//        try {
//            LOG.info("Tag Lookup cache:" + tagId);
//            TagMetaData tagMetaData = CacheUtil2.getTagToMetaDataLookupCache(tagId);
//            if(tagMetaData == null){
//                tagMetaData = metaLookup.getTagMetaDataforTag(tagId);
//                CacheUtil2.putTagToTagMetaDataLookupCache(tagId, tagMetaData);
//            }

            // Check for alarms (when value is above or below high and low)
            if(temp > ALERT_LEVEL) {
                String timeString = new DateTime(new Date(traceTime)).toString();
                String message = String.format("Alert Threshold = %10.3f\nTraceTime: %s Depth: %10.3f Temp: %10.3f",
                        ALERT_LEVEL, timeString, depth, temp);
                alertMail.sendSmtpMail(message);
                alert = true;
                LOG.debug("### Alert reported ###");
            }
//        } catch (IOException e) {
//            LOG.error("Error while logging alert: " + e);
//            e.printStackTrace();
//        }
        LOG.debug("END AlertBolt.validateAndLogAlert()");

        return alert;
    }


//    private void logAlert(String tagId, int pumpIdNo, long eventTime, double value, double quality, String comment, AlertTypeEnum alertTypeEnum) throws IOException {
//        LOG.info("START AlertBolt.logAlert()");
//        LOG.info("##### Alert reported: type:" + alertTypeEnum.getType());
//        try {
//            StringBuilder stringBuilder = new StringBuilder();
//            stringBuilder.append("This is a test alert of type: " + alertTypeEnum.getType())
//                    .append(". eventTime=" + eventTime)
//                    .append("|value=" + value)
//                    .append("|quality=" + quality)
//                    .append("|comment=" + comment);
//
//            if(tagAlert==null){
//                tagAlert = new TagAlert();
//            }
//            tagAlert.put(tagId, pumpIdNo, alertTypeEnum, stringBuilder.toString(), tagAlertsTable);
//        } catch (IOException e) {
//            LOG.error("Error inserting event into HBase table[" + TAG_ALERTS_TABLE_NAME + "]", e);
//            throw e;
//        }
//        LOG.info("END AlertBolt.logAlert()");
//
//    }



    @Override
    public void cleanup()
    {
//        try
//        {
//            tagAlertsTable.flushCommits();
//            tagAlertsTable.close();
//        }
//        catch (Exception  e)
//        {
//            LOG.error("Error closing tables", e);
//        }
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
