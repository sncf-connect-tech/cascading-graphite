package com.vsct.aop.hadoop.cascading.graphite;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by guillaume_dufour on 19/02/15.
 */
public class GraphiteTapCollector<Config> extends TupleEntrySchemeCollector implements OutputCollector {

    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteTapCollector.class);

    /** Field conf */
    private final Config conf;
    /** Field writer */
    private RecordWriter writer;
    /** Field flowProcess */
    private final FlowProcess<Config> hadoopFlowProcess;
    /** Field tap */
    private final SinkTap<Config, OutputCollector> tap;
    /** Field reporter */
    private final Reporter reporter = Reporter.NULL;


    /**
     * Constructor TapCollector creates a new TapCollector instance.
     *
     * @param flowProcess process which use this collector
     * @param tap output tap
     * @throws IOException when fails to initialize
     */
    public GraphiteTapCollector( FlowProcess<Config> flowProcess, SinkTap<Config, OutputCollector> tap ) throws IOException
    {
        super( flowProcess, tap.getScheme() );
        this.hadoopFlowProcess = flowProcess;

        this.tap = tap;
        this.conf = flowProcess.getConfigCopy();

        this.setOutput( this );
    }

    @Override
    public void prepare()
    {
        try
        {
            initialize();
        }
        catch( IOException e )
        {
            throw new CascadingException( e );
        }
        super.prepare();
    }

    private void initialize() throws IOException
    {
        tap.sinkConfInit( hadoopFlowProcess, conf );
        if(conf instanceof JobConf) {
            OutputFormat outputFormat = ((JobConf) conf).getOutputFormat();
            LOG.info("Output format class is: " + outputFormat.getClass().toString());
            writer = outputFormat.getRecordWriter(null, (JobConf)conf, tap.getIdentifier(), Reporter.NULL);
        } else if(conf instanceof Properties){
            GraphiteOutputFormat outputFormat = new GraphiteOutputFormat();
            writer = outputFormat.getRecordWriter((Properties)conf);
        }
        
        sinkCall.setOutput( this );
    }

    @Override
    public void close()
    {
        try
        {
            LOG.info( "closing tap collector for: {}", tap );
            writer.close( reporter );
        }
        catch( IOException exception )
        {
            LOG.error( "exception closing: {}", exception.getMessage(), exception );
            throw new TapException( "exception closing GraphiteTapCollector", exception );
        }
        finally
        {
            super.close();
        }
    }

    /**
     * Method collect writes the given values to the {@link Tap} this instance encapsulates.
     *
     * @param writableComparable of type WritableComparable
     * @param writable           of type Writable
     * @throws IOException when
     */
    @Override
    public void collect( Object writableComparable, Object writable ) throws IOException
    {
        if( hadoopFlowProcess instanceof HadoopFlowProcess )
            ( (HadoopFlowProcess) hadoopFlowProcess ).getReporter().progress();

        writer.write( writableComparable, writable );
    }
}
