package com.hortonworks.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import scala.actors.threadpool.Arrays;

import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Created by pdeshmukh on 11/13/14.
 */
public class TagEventProducer {
    private static final Logger LOG = Logger.getLogger(TagEventProducer.class);
    private static final int EVENT_COUNT = 5;

//    private static final String zkCconnect="192.168.1.16:2181";
    private static String brokerList="192.168.1.9:9092";
    private static String topic="tagevent55";

    //static data for sample events
    private static final Integer TAGS[] = {1200, 1201, 1220, 1221, 1230,1231};
    private static final Integer PUMP[] = {10001};
    private static final String METRICS[] = {"METRIC1", "METRIC2", "METRIC3","METRIC4","METRIC5","METRIC6","METRIC7","METRIC8","METRIC9", "METRIC10"};

    public static void main(String args[]){
        LOG.info("START - TagEventProducer");

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


        Producer producer = getProducer();
        StringBuilder evntBuilder = null;
        // tag_id|timestamp|
        Random randomGenerator = new Random();
        for(int i=0; i<eventCount; i++) {
            DateTime dateTime = new DateTime();
            for(Integer aTag : (List<Integer>)Arrays.asList(TAGS)) {
                for(String aMetric : (List<String>)Arrays.asList(METRICS)) {
                    evntBuilder = new StringBuilder();
                    evntBuilder.append(aTag).append("|")
                            .append(dateTime.getMillis()).append("|")
                            .append(aMetric).append("|")
                            .append(randomGenerator.nextInt(1000));
                    KeyedMessage<String, String> message =
                            new KeyedMessage<String, String>(topic, evntBuilder.toString());
                    LOG.info("Sending message: TOPIC: " + topic + "Event: " + evntBuilder.toString());
                    producer.send(message);
                }
            }
            //Sleep 20
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

//        producer.send();
        LOG.info("STOP - TagEventProducer");
    }

    private static Producer<String, String> getProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
//        props.put("zk.connect", zkCconnect);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        props.put("producer.type", "sync");
//        props.put("squeue.enqueue.timeout.ms", "-1");
//        props.put("batch.num.messages", "200");


        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        return producer;
    }
}
