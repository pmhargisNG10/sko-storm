package com.hortonworks.common.util;

import com.hortonworks.hbase.access.TagEventData;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by dpramodv on 12/31/14.
 */
public class HUtil {
    private static Logger log = Logger.getLogger(HUtil.class);
    public static void main(String args[]) throws IOException {
        System.out.println("START HUtil.main()");
        List<String> allRowKeys = TagEventData.getAllRowKeys();
        for (String rowKey : allRowKeys){
            System.out.println(rowKey);
        }
        System.out.println("END HUtil.main()");

    }

}
