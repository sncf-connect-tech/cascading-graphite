package com.vsct.aop.hadoop.cascading.graphite;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * A Scheme represents a format or representation, like a text format for a file, or columns in a table.
 * So it represente the 3 columns format with  key, value and timestamp.
 */
public class GraphiteSinkScheme<Config> extends Scheme<Config, Void, OutputCollector, Void, Object[]> {

    private String graphiteKeyField;
    private String valueField;
    private String timestampField;

    public GraphiteSinkScheme() throws IOException {
    }

    @Override
    public void sourceConfInit(FlowProcess<Config> flowProcess, Tap<Config, Void, OutputCollector> tap, Config conf) {
        throw new NotImplementedException();
    }

    public GraphiteSinkScheme(String graphiteKeyField, String valueField, String timestampField) throws IOException {
        this.graphiteKeyField = graphiteKeyField;
        this.timestampField = timestampField;
        this.valueField = valueField;
    }
    @Override
    public void sinkConfInit(FlowProcess<Config> flowProcess,
                             Tap<Config, Void, OutputCollector> tap, Config conf) {
        if(conf instanceof JobConf)
            ((JobConf)conf).setOutputFormat( GraphiteOutputFormat.class );

    }

    @Override
    public boolean source(FlowProcess<Config> flowProcess, SourceCall<Void, Void> sourceCall) throws IOException {
        return false;
    }

    @Override
    public void sink(FlowProcess<Config> flowProcess,
                     SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        // it's ok to use NULL here so the collector does not write anything
        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
        OutputCollector outputCollector = sinkCall.getOutput();
        if(graphiteKeyField != null && graphiteKeyField.length()>0 && valueField != null && valueField.length()>0 && timestampField!=null && timestampField.length()>0)
            outputCollector.collect( new GraphiteWritable( tupleEntry, graphiteKeyField, valueField, timestampField), null );
        else
            outputCollector.collect( new GraphiteWritable( tupleEntry ), null );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        GraphiteSinkScheme that = (GraphiteSinkScheme) o;

        if (graphiteKeyField != null ? !graphiteKeyField.equals(that.graphiteKeyField) : that.graphiteKeyField != null)
            return false;
        if (timestampField != null ? !timestampField.equals(that.timestampField) : that.timestampField != null)
            return false;
        if (valueField != null ? !valueField.equals(that.valueField) : that.valueField != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (graphiteKeyField != null ? graphiteKeyField.hashCode() : 0);
        result = 31 * result + (valueField != null ? valueField.hashCode() : 0);
        result = 31 * result + (timestampField != null ? timestampField.hashCode() : 0);
        return result;
    }
}
