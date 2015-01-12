package com.slb.storm.pump;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by dpramodv on 11/14/14.
 */
public class ESPEventTopology extends BaseEventTopology {
    private static final String KAFKA_SPOUT_ID = "kafkaSpout";
    private static final String MONITOR_BOLT_ID = "monitorBolt";
    private static final String LOOKUP_BOLT_ID = "lookupBolt";
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
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout, 25);
    }

    public void configureLookupBolt(TopologyBuilder builder)
    {
        TagLookupBolt tagLookupBolt = new TagLookupBolt();
//        builder.setBolt(LOOKUP_BOLT_ID, tagLookupBolt, 2).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(LOOKUP_BOLT_ID, tagLookupBolt, 30).fieldsGrouping(KAFKA_SPOUT_ID,
                new Fields(ESPScheme.TAG_ID));
    }

    public void configureMonitorBolt(TopologyBuilder builder)
    {
        MonitorBolt monitorBolt = new MonitorBolt();
//        builder.setBolt(MONITOR_BOLT_ID, monitorBolt, 2).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(MONITOR_BOLT_ID, monitorBolt, 40).fieldsGrouping(LOOKUP_BOLT_ID,
                new Fields(ESPScheme.PUMP_ID));
    }

    public void configureHBaseBolt(TopologyBuilder builder)
    {
        TagHBaseBolt hbaseBolt = new TagHBaseBolt();
        builder.setBolt(HBASE_BOLT_ID, hbaseBolt, 42).fieldsGrouping(LOOKUP_BOLT_ID,
                new Fields(ESPScheme.PUMP_ID));
    }

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
        configureLookupBolt(builder);
        configureHBaseBolt(builder);
        configureMonitorBolt(builder);

//        StormSubmitter.submitTopology("tag-event-processor",
//                conf, builder.createTopology());

        StormSubmitter.submitTopology(topologyConfig.getProperty("storm.topology.name"),
                conf, builder.createTopology());
    }

    public static void main(String[] str) throws Exception
    {
        String configFileLocation = "tag_event_slb.properties";
//        String configFileLocation = "tag_event_hmap.properties";
        ESPEventTopology truckTopology
                = new ESPEventTopology(configFileLocation);
        truckTopology.buildAndSubmit();
    }
}