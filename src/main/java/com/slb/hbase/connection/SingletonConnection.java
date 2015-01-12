package com.slb.hbase.connection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;


public class SingletonConnection {

    private static HConnection connection = null;
    private static Object obj = new Object();

    private SingletonConnection() {
        synchronized(obj) {
            if (connection == null) {
                Configuration conf = HBaseConfiguration.create();
                try {
                    connection = HConnectionManager.createConnection(conf);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }


    public static HTableInterface getInstance(String tablename) throws IOException{
        if (connection == null) {
            synchronized (obj) {
                new SingletonConnection();
            }
        }
        HTableInterface table = connection.getTable(tablename);
        table.setAutoFlushTo(false);
        return table;
    }
}

