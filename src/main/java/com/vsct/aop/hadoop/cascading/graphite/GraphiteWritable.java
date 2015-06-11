package com.vsct.aop.hadoop.cascading.graphite;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.io.PrintWriter;

/**
 * Data well formatted for graphite
 */
public class GraphiteWritable {
    String key;
    String value;
    long timestamp;
    
    public GraphiteWritable(){}

    public GraphiteWritable(TupleEntry te, String keyField, String valueField, String timestampField){
        key = te.getString(keyField);
        value = te.getString(valueField);
        timestamp = te.getLong(timestampField);
    }

    public GraphiteWritable(TupleEntry te){
        key = te.getTuple().getString(0);
        value = te.getTuple().getString(1);
        timestamp = te.getTuple().getLong(2);
    }
    
    /**
     * write fields of the object in the {@link PrintWriter}.
     * @param sb stringbuffer where append graphite formated data
     * @param root prefix of the graphite key, optionnal so it can be null
     */
    public void write(StringBuffer sb, String root) {
        if(root != null && root.length() > 0) {
            sb.append(root);
            sb.append(".");
        }
        if(key != null && key.length() > 0) {
            sb.append(key);
            sb.append(" ");
            sb.append(value);
            sb.append(" ");
            sb.append(timestamp);
            sb.append("\n");
        }
    }
}
