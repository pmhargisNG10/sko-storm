package com.hortonworks.common.util;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: phargis
 * Date: 1/20/15
 * Time: 10:37 AM
 */
public class PropertiesLoader {

    private static final Logger LOG = Logger.getLogger(PropertiesLoader.class);

    public static Properties loadPropertiesFromFile(String propertiesFilename) throws Exception {
        Properties properties = new Properties();
        try {
            properties.load(ClassLoader.getSystemResourceAsStream(propertiesFilename));
        } catch (FileNotFoundException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        } catch (IOException e) {
            LOG.error("Encountered error while reading configuration properties: "
                    + e.getMessage());
            throw e;
        }
        return properties;
    }
}
