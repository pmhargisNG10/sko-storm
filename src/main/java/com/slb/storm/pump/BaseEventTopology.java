package com.slb.storm.pump;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by dpramodv on 11/14/14.
 */
public class BaseEventTopology {
    private static final Logger LOG = Logger.getLogger(BaseEventTopology.class);

    protected Properties topologyConfig;

    public BaseEventTopology(String configFileLocation) throws Exception {
        topologyConfig = new Properties();
        try {
            topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
        } catch (FileNotFoundException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        }
    }
}
