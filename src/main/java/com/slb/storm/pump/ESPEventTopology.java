package com.slb.storm.pump;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by Paul Hargis
 */
public class ESPEventTopology extends BaseEventTopology {

    private static final Logger LOG = Logger.getLogger(ESPEventTopology.class);

    private static final String KAFKA_SPOUT_ID = "kafkaSpout";
    private static final String ALERT_BOLT_ID = "dtsAlertBolt";
    private static final String DTS_BOLT_ID = "dtsMonitorBolt";
    private static final String HBASE_BOLT_ID = "hbaseBolt";

    public ESPEventTopology(String configFileLocation) throws Exception{
        super(configFileLocation);
    }

    private SpoutConfig constructKafkaSpoutConf()
    {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.topic");
        String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
        String consumerGroupId = "storm";

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

        spoutConfig.scheme = new SchemeAsMultiScheme(new ESPScheme());

        return spoutConfig;
    }

    public void configureKafkaSpout(TopologyBuilder builder)
    {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout, 3);
    }

    public void configureEventBolt(TopologyBuilder builder)
    {
        DtsEventBolt dtsEventBolt = new DtsEventBolt();
        builder.setBolt(DTS_BOLT_ID, dtsEventBolt, 3).fieldsGrouping(KAFKA_SPOUT_ID,
                new Fields(ESPScheme.TRACE_KEY));
    }

    public void configureAlertBolt(TopologyBuilder builder)
    {
        DtsAlertBolt dtsAlertBolt = new DtsAlertBolt();
//        builder.setBolt(ALERT_BOLT_ID, monitorBolt, 2).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(ALERT_BOLT_ID, dtsAlertBolt, 21).fieldsGrouping(DTS_BOLT_ID,
                new Fields(ESPScheme.TRACE_KEY));
    }

//    public void configureHBaseBolt(TopologyBuilder builder)
//    {
//        TagHBaseBolt hbaseBolt = new TagHBaseBolt();
//        builder.setBolt(HBASE_BOLT_ID, hbaseBolt, 42).fieldsGrouping(DTS_BOLT_ID,
//                new Fields(ESPScheme.TRACE_KEY));
//    }

    private void buildAndSubmit() throws Exception
    {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(21); // 3 workers * 7 supervisors
        /**
         * 7 supervisors
         * 7 supervisors
         * 8 cores: each handling 2 threads. (2 * 8 * 7 sup) = 112
         */

        TopologyBuilder builder = new TopologyBuilder();
        configureKafkaSpout(builder);
        configureEventBolt(builder);
        configureAlertBolt(builder);
        //configureHBaseBolt(builder);

        StormSubmitter.submitTopology(topologyConfig.getProperty("storm.topology.name"),
                conf, builder.createTopology());
    }

    public static void main(String[] str) throws Exception
    {
        String stormConfigFile = "sko_storm_demo.properties";
        ESPEventTopology truckTopology = new ESPEventTopology(stormConfigFile);
        truckTopology.buildAndSubmit();
    }

}