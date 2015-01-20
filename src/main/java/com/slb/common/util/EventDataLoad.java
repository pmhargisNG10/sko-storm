package com.slb.common.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.slb.common.data.TagEvent;
import com.slb.hbase.lookup.MetaLookup;
import com.slb.hbase.persist.MetaData;
import com.slb.hbase.persist.TagPersist;
import com.slb.kafka.producer.JSONEventProducer;
import kafka.javaapi.producer.Producer;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;
/**
 * Created by dpramodv on 11/30/14.
 */
public class EventDataLoad {
    private static Logger LOG = Logger.getLogger(EventDataLoad.class);
    public static void main(String[] args) throws IOException {
        System.out.println("START EventDataLoad");
//        loadCSVData();
        loadJSONData(args);
        System.out.println("END EventDataLoad");
    }

    /**
     * Used for test data load
     * @param args
     * @throws IOException
     */
    private static void loadJSONData(String args[]) throws IOException {
        System.out.println("START EventDataLoad.loadJSONData()");

        String brokerList = "bumblebee3.binarylore.com:6667,bumblebee4.binarylore.com:6667,cdminsight.binarylore.com:6667";
        String topic =  "pocTagEvents2";

        LOG.info("Broker List: " + brokerList);
        LOG.info("KAFKA TOPIC=" + topic);
        Date statTime = new Date(System.currentTimeMillis());
        Random random = new Random();
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'").create();
        Path path = null;
        //command line args: <file path> <kafka topic> <broker list>
        if(args.length > 0){
            path = FileSystems.getDefault().getPath(args[0]);
            topic = args[1];
            brokerList = args[2];
        } else {
            path = Paths.get(MetaDataLoad.class.getResource("/rt_json_sample_data.txt").getPath());
        }

        System.out.println("KAFKA TOPIC=" + topic);
        System.out.println("KAFKA BROKERS=" + brokerList);

        BufferedReader reader = Files.newBufferedReader(path,
                StandardCharsets.UTF_8);
        String line = null;
        Producer<String, String> producer = JSONEventProducer.getProducer(brokerList);
        long count = 0;

        List<TagEvent> tagEventList = new ArrayList<TagEvent>();
        while ((line = reader.readLine())!= null) {
            TagEvent tagEvent = gson.fromJson(line, TagEvent.class);
            tagEventList.add(tagEvent);
        }



        for(int i=0; i<800000; i++) {
            int tagEventPos = random.nextInt(tagEventList.size() - 1);
            TagEvent tagEvent = tagEventList.get(tagEventPos);
//            System.out.println("TagEvent: " + tagEvent.getTagId());
            //set time and pick random value
            DateTime dateTime = new DateTime(tagEvent.getTime().getTime());
//                System.out.println("Previous date:" + dateTime.getMillis());
            Date newDate = new Date(dateTime.plusMillis(20).getMillis());
//                System.out.println("New date:" + newDate.getTime());
            tagEvent.setTime(newDate);
            tagEvent.setDoubleValue(random.nextDouble());
//                System.out.println(gson.toJson(tagEvent));
            tagEventList.remove(tagEventPos);
            tagEventList.add(tagEventPos, tagEvent);
            JSONEventProducer.send(producer, topic, gson.toJson(tagEvent).toString());
            count ++;
        }

        Date endTime = new Date(System.currentTimeMillis());

        System.out.println("####### TOTAL JSON RECORDS PRODUCED: " + count);
        System.out.println("Start Time: " + statTime);
        System.out.println("End Time: " + endTime);
        System.out.println("END EventDataLoad.loadJSONData()");
    }

    @Deprecated
    private static void loadCSVData(String... args) throws IOException{
//        Path path = Paths.get(MetaDataLoad.class.getResource("/event_data_100.csv").getPath());
        Path path = Paths.get(MetaDataLoad.class.getResource("/sample_data.csv").getPath());

        MetaData metaData = new MetaData();
        MetaLookup metaLookup = new MetaLookup();
        try {
            BufferedReader reader = Files.newBufferedReader(path,
                    StandardCharsets.UTF_8);
            String line = null;
            TagPersist tagPersist = new TagPersist();
//            TagRowKey tagRowKey = new TagRowKey();
//            List<String> stringList = new ArrayList<String>(100);
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
//                System.out.println(line);
//                Pump pump = new Pump();
//                pump.setPumpId(fields[0]);
//                int pumpIdNo = metaData.getPumpId(pump);

                //1,Accel_Pump_head_90,2014-10-16T18:25:12.0058824Z,-3.456601779,0,null
                int pumpId = Integer.parseInt(fields[0]);
                String tagname = fields[1];
                String time = fields[2];
                double value = Double.parseDouble(fields[3]);
                double quality = Double.parseDouble(fields[4]);
                String comment = fields[5];
                int tagIdNo = metaLookup.getTagIdforTag(tagname);

                DateTime dateTime = new DateTime(time);
                GregorianCalendar calendar = dateTime.toDateTime(DateTimeZone.UTC).toGregorianCalendar();
                long timeInMillis = calendar.getTimeInMillis();

//                System.out.println("utcDate === " + timeInMillis);
//                System.out.println("calendar === " + calendar.getTime());

                try {
                    byte rowkey[] = tagPersist.writeTagValues(tagIdNo, pumpId, timeInMillis, value, quality, comment);

//                    long[] keyD = tagRowKey.deconstructRowkey1(rowkey);
//                    stringList.add("RowKey=>" + keyD[0] + ":" + keyD[1]+ ":" + keyD[2] + "|Data=" + line);

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

//            for(String a: stringList) {
//                System.out.println(a);
//            }

            reader.close();
        } catch (IOException ioe) {
            System.err.println(ioe);
        }
        System.out.println("END EventDataLoad");
    }
}
