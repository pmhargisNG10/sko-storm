package com.hortonworks.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * Created by pdeshmukh on 11/13/14.
 */
public class JSONEventProducer {

    /**
     * dev cluster:
     * params: 100 pocTagEvents bumblebee3.binarylore.com:6667,bumblebee4.binarylore.com:6667,cdminsight.binarylore.com:6667
     * zk: ironhide1.binarylore.com:2181,rodimus1.binarylore.com:2181,rodimus2.binarylore.com:2181
     * brokerlist: bumblebee3.binarylore.com:6667,bumblebee4.binarylore.com:6667,cdminsight.binarylore.com:6667
     *
     * slb szure cluster:
     * zk: xithdp3-master-02.cloudapp.net:2181,xithdp3-master-01.cloudapp.net:2181,xithdp3-master-03.cloudapp.net:2181
     * 100 poctagevents xithdp3-master-02.cloudapp.net:2181,xithdp3-master-01.cloudapp.net:2181,xithdp3-master-03.cloudapp.net:2181
     * brokerlist: xithdp3-worker-01.cloudapp.net:9092,xithdp3-worker-02.cloudapp.net:9092,xithdp3-worker-03.cloudapp.net:9092
     */
    private static final Logger LOG = Logger.getLogger(JSONEventProducer.class);
    private static final int EVENT_COUNT = 5;
//    private static final String zkCconnect="192.168.1.16:2181";
    private static String brokerList="192.168.1.9:9092";
    private static String topic="tagevent55";


    public static void main(String args[]) throws Exception {
        LOG.info("START - TagEventProducer");
        if(args != null && args.length < 3){
            LOG.error("Not enough arguments");
            return;
        }
        int eventCount = EVENT_COUNT;
        if(args[0] != null){
            eventCount = Integer.parseInt(args[0]);
        }
        if(args[1] != null){
            topic = args[1];
        }
        if(args[2] != null){
            brokerList = args[2];
        }

        System.out.println("Event count=" + eventCount + " Topic=" + topic + " Broker List=" + brokerList);
        Producer producer = getProducer(brokerList);
//        String url = JSONEventProducer.class.getResource("/rt_sample_data.txt").getPath();
        String url = JSONEventProducer.class.getResource("/rt_sample_data-pump2-ERR_Alerts.txt").getPath();
//        String url = JSONEventProducer.class.getResource("/rt_sample_data-pump2-WARN_Alerts.txt").getPath();
//        String url = JSONEventProducer.class.getResource("/rt_events_low_alerts_data.txt").getPath();
//        String url = JSONEventProducer.class.getResource("/rt_events_hi_alerts_data.txt").getPath();
        File file = new File(url);
        List<String> lines = FileUtils.readLines(file, "UTF-8");
        for(String aline: lines) {
            send(producer, topic, aline);
        }

        LOG.info("STOP - TagEventProducer");
    }

    public static Producer<String, String> getProducer(String brokerList) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
//        props.put("zk.connect", zkCconnect);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "-1");

        props.put("producer.type", "sync");
//        props.put("squeue.enqueue.timeout.ms", "-1");
//        props.put("batch.num.messages", "200");


        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        return producer;
    }

    public static void send(Producer<String, String> producer, String topic, String jsonString) {
        KeyedMessage<String, String> message =
                new KeyedMessage<String, String>(topic, jsonString);
        LOG.info("Sending message: TOPIC: " + topic + "Event: " + jsonString);
        producer.send(message);
    }
}
