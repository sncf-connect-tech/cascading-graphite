package com.vsct.aop.hadoop.cascading.graphite;

import cascading.flow.FlowProcess;
import cascading.tap.SinkTap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.Properties;

/**
 * According to the documentation, a Tap represents something "physical", like a file or a database table.
 */
public class GraphiteSinkTap<Config> extends SinkTap<Config, OutputCollector> implements
        Serializable {
    
    private static final long serialVersionUID = 1L;

    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteSinkTap.class);
    
    private String identifier;

    private String host;
    private int port;
    private String root;

    public GraphiteSinkTap(GraphiteSinkScheme scheme, String identifier, String host, int port, String root) {
        super(scheme);
        this.identifier = identifier;
        this.host = host;
        this.port = port;
        this.root = root;
    }

    @Override
    public void sinkConfInit(FlowProcess<Config> flowProcess, Config conf) {
        if( !isSink() )
            return;
        if(conf instanceof Configuration) {
            ((Configuration)conf).setBoolean("mapreduce.reduce.speculative", false);
        }
        if( host != null && host.length()>0 ) {
            if(conf instanceof Configuration) {
                GraphiteConfiguration.configureGraphiteForHadoop((Configuration) conf, host, port, root);
            } else if(conf instanceof Properties){
                GraphiteConfiguration.configureGraphiteInLocal((Properties) conf, host, port, root);
            }
        }
        super.sinkConfInit(flowProcess, conf);
    }
    @Override
    public String getIdentifier() {
        return this.identifier;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Config> flowProcess,
                                            OutputCollector output) throws IOException {
        if( !isSink() )
            throw new TapException( "this tap may not be used as a sink" );
        LOG.info( "Creating GraphiteTapCollector output instance" );
        GraphiteTapCollector graphiteTapCollector = new GraphiteTapCollector( flowProcess, this );
        graphiteTapCollector.prepare();
        return graphiteTapCollector;
    }

    @Override
    public boolean createResource(Config conf) throws IOException {
        return false;
    }

    @Override
    public boolean deleteResource(Config conf) throws IOException {
        return false;
    }

    @Override
    public boolean resourceExists(Config conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(Config conf) throws IOException {
        return new Date().getTime();
    }
}
