package com.vsct.aop.hadoop.cascading.graphite;

import java.io.IOException;
import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphiteTapFactory {

    private static final Logger LOG = LoggerFactory.getLogger(GraphiteTapFactory.class);
    public static final String DEFAULT_SEPARATOR = ":";
    public static final String PROTOCOL_GRAPHITE_HOST = "graphiteHost";
    public static final String PROTOCOL_GRAPHITE_PORT = "graphitePort";
    public static final String PROTOCOL_GRAPHITE_ROOT = "graphiteRoot";

    public Tap createTap(Scheme scheme, Properties properties)
            throws IOException {

        String graphiteHost = properties.getProperty( PROTOCOL_GRAPHITE_HOST );
        int graphitePort = Integer.parseInt(properties.getProperty( PROTOCOL_GRAPHITE_PORT ));
        String graphiteRoot = properties.getProperty( PROTOCOL_GRAPHITE_ROOT );

        GraphiteSinkScheme graphiteScheme = (GraphiteSinkScheme) scheme;
        
        return createTap(graphiteScheme, graphiteHost, graphitePort, graphiteRoot);
    }

    public Tap createTap(String graphiteKeyField, String valueField, String timestampField, String graphiteHost, int graphitePort, String graphiteRoot)
            throws IOException {

        GraphiteSinkScheme graphiteScheme = new GraphiteSinkScheme(graphiteKeyField, valueField, timestampField);

        return createTap(graphiteScheme, graphiteHost, graphitePort, graphiteRoot);
    }

    public Tap createTap(GraphiteSinkScheme graphiteScheme, String graphiteHost, int graphitePort, String graphiteRoot)
            throws IOException {

        return new GraphiteSinkTap(graphiteScheme, graphiteHost+":"+graphitePort, graphiteHost, graphitePort, graphiteRoot);
    }
    
}
