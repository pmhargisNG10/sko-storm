package com.hortonworks.hbase.lookup;

import com.hortonworks.common.data.Alert;
import com.hortonworks.common.data.AlertTypeEnum;
import com.hortonworks.common.data.TagMetaData;
import com.hortonworks.hbase.connection.SingletonConnection;
import com.hortonworks.hbase.persist.TagAlert;
import com.hortonworks.hbase.rowkeys.AlertRowKey;
import com.hortonworks.storm.pump.ESPScheme;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by dpramodv on 12/4/14.
 */
public class AlertLookup {
    private static Logger LOG = Logger.getLogger(AlertLookup.class);

    private HTableInterface table;

    public AlertLookup(){
        this.createTableInstance();
    }

    public void createTableInstance(){
        synchronized (this) {
            try {
                table = SingletonConnection.getInstance(TagAlert.TAG_ALERTS_TABLE_NAME);
            } catch (IOException e) {
                LOG.error("Error while getting instance for table: " + TagAlert.TAG_ALERTS_TABLE_NAME);
            }
        }
    }

    public List<Alert> getAlertsByPump(int pumpId, AlertTypeEnum alertType) throws IOException {
        byte[] skey = AlertRowKey.constructRowKey(pumpId, null, alertType, null);
        byte[] ekey = AlertRowKey.constructRowKey(pumpId+1, null, alertType, null);

        Scan scan = new Scan(skey, ekey);
        if(table == null){
            createTableInstance();
        }
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        List<Alert> alertList = new ArrayList<Alert>();
        Alert alert = null;
        while(iterator.hasNext()){
            Result result = iterator.next();
            String type = Bytes.toString(result.getValue(TagAlert.CF_TAG_ALERT, Alert.ALERT_TYPE));
            String message = Bytes.toString(result.getValue(TagAlert.CF_TAG_ALERT, Alert.MESSAGE));
            String tagId = Bytes.toString(result.getValue(TagAlert.CF_TAG_ALERT, Bytes.toBytes(ESPScheme.TRACE_KEY)));
//            int pumpIdNoStr = Bytes.toInt(result.getValue(TagAlert.CF_TAG_ALERT, Bytes.toBytes(ESPScheme.PUMP_ID_NO)));
            alert = new Alert();
            alert.setAlertType(type);
            alert.setMessage(message);

            //Get tag meta data for
            MetaLookup metaLookup = new MetaLookup();
            TagMetaData tagMetaData = metaLookup.getTagMetaDataforTag(tagId);

            alert.setTagMetaData(tagMetaData);

            alertList.add(alert);
        }

        return alertList;
    }

    public List<Alert> getAlertsByPumpAndTag(int pumpId, String tagId, AlertTypeEnum alertType){

        return null;
    }

    public static void main(String... args) throws IOException {
        LOG.info("START AlertLookup.main()");

        AlertLookup alertLookup = new AlertLookup();
        List<Alert> alertList = alertLookup.getAlertsByPump(1, AlertTypeEnum.WARNING);
        for(Alert alert: alertList){
            LOG.info(alert);
        }

        LOG.info("END AlertLookup.main()");
    }

}
