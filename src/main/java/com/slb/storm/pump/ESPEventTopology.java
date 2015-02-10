package com.slb.storm.pump;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
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
    private static final String HDFS_BOLT_ID = "hdfsBolt";
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

    public void configureHDFSBolt(TopologyBuilder builder)
    {
//        String rootPath = topologyConfig.getProperty("hdfs.path");
//        String prefix = topologyConfig.getProperty("hdfs.file.prefix");
//        String fsUrl = topologyConfig.getProperty("hdfs.url");
//        String sourceMetastoreUrl = topologyConfig.getProperty("hive.metastore.url");
//        String hiveStagingTableName = topologyConfig.getProperty("hive.staging.table.name");
//        String databaseName = topologyConfig.getProperty("hive.database.name");
//        Float rotationTimeInMinutes = Float.valueOf(topologyConfig.getProperty("hdfs.file.rotation.time.minutes"));

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(",")
                .withFields(new Fields(ESPScheme.TRACE_KEY, ESPScheme.DEPTH_KEY, ESPScheme.TEMP_KEY));

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/tmp/storm-demo/");

        // Instantiate the HdfsBolt
        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://hdppmh1-master-01.cloudapp.net:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        // int hdfsBoltCount = Integer.valueOf(topologyConfig.getProperty("hdfsbolt.thread.count"));
        builder.setBolt(HDFS_BOLT_ID, bolt, 3).shuffleGrouping(KAFKA_SPOUT_ID);
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
        configureHDFSBolt(builder);
        //configureHBaseBolt(builder);

        StormSubmitter.submitTopology(topologyConfig.getProperty("storm.topology.name"),
                conf, builder.createTopology());
    }

    public static void main(String[] str) throws Exception
    {
        String stormConfigFile = "storm_demo.properties";
        ESPEventTopology espEventTopology = new ESPEventTopology(stormConfigFile);
        espEventTopology.buildAndSubmit();
    }

}