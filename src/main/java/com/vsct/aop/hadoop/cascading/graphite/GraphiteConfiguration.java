package com.vsct.aop.hadoop.cascading.graphite;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * Created by guillaume_dufour on 19/02/15.
 */
public class GraphiteConfiguration<Config> {

    public static final String HOST_PROPERTY = "mapred.graphite.host";
    public static final String PORT_PROPERTY = "mapred.graphite.port";
    public static final String ROOT_PROPERTY = "mapred.graphite.root";
    public static final String BUFFER_SIZE_PROPERTY = "mapred.graphite.buffer.size";
    public static final String TRIGGER_SIZE_PROPERTY = "mapred.graphite.trigger.size";

    private static int DEFAULT_BUFFER_SIZE = 25 * 1024;
    private static int DEFAULT_TRIGGER_SIZE = 24 * 1024;

    private Config conf;

    GraphiteConfiguration(Config conf) {
        this.conf = conf;
    }

    public static void configureGraphiteForHadoop(Configuration conf, String graphiteHost,
                                         int graphitePort, String graphiteRoot) {
        conf.set(HOST_PROPERTY, graphiteHost);
        conf.set(PORT_PROPERTY, String.valueOf(graphitePort));
        conf.set(ROOT_PROPERTY, graphiteRoot);
    }

    public static void configureGraphiteInLocal(Properties conf, String graphiteHost,
                                         int graphitePort, String graphiteRoot) {
        conf.setProperty(HOST_PROPERTY, graphiteHost);
        conf.setProperty(PORT_PROPERTY, String.valueOf(graphitePort));
        conf.setProperty(ROOT_PROPERTY, graphiteRoot);
    }


    String getHost() {
        return getString(GraphiteConfiguration.HOST_PROPERTY, "localhost");
    }

    int getPort() {
        return getInt(GraphiteConfiguration.PORT_PROPERTY,8080);
    }

    private String getString(String key, String defaultValue) {
        if(conf instanceof Configuration)
            return ((Configuration)conf).get(key, defaultValue);
        else
            return ((Properties)conf).getProperty(key, defaultValue);

    }

    private int getInt(String key, int defaultValue) {
        if(conf instanceof Configuration)
            return ((Configuration)conf).getInt(key, defaultValue);
        else
            return getLocalInt(key, defaultValue);

    }
    
    private int getLocalInt(String key, int defaultValue) {
        try {
            String valueString = ((Properties) conf).getProperty(key, String.valueOf(defaultValue));
            return Integer.parseInt(valueString);
        } catch (NumberFormatException var5) {
            return defaultValue;
        }
        
    }

    String getRoot() {
       return getString(GraphiteConfiguration.ROOT_PROPERTY, null);
    }

    int getBufferSize() {
        return getInt(GraphiteConfiguration.BUFFER_SIZE_PROPERTY, DEFAULT_BUFFER_SIZE);
    }

    int getTriggerSize() {
        return getInt(GraphiteConfiguration.TRIGGER_SIZE_PROPERTY, DEFAULT_TRIGGER_SIZE);
    }
}
