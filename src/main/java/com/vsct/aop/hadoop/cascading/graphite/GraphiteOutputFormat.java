package com.vsct.aop.hadoop.cascading.graphite;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by guillaume_dufour on 19/02/15.
 */
public class GraphiteOutputFormat<K extends GraphiteWritable, V> implements OutputFormat<K, V> {

    protected class GraphiteRecordWriter implements RecordWriter<K, V>{

        private transient PrintWriter pw;
        private String root;
        private StringBuffer sb;
        private final int triggerSize;
        
        public GraphiteRecordWriter(PrintWriter pw, String root, int bufferSize, int triggerSize) {
            this.pw = pw;
            this.root = root;
            this.sb = new StringBuffer(bufferSize);
            this.triggerSize = triggerSize;
        }

        @Override
        public void write(K key, V value) throws IOException {
            
            key.write(sb, root);

            if (sb.length() > triggerSize) {
                flush();
            }
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            try
            {
                if(sb.length() > 0)
                    flush();
            } finally {
                if(pw != null)
                    pw.close();
            }
        }

        private void flush() {
            pw.println(sb.toString());
            sb.setLength(0);
            pw.flush();
        }
    }
    
    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        return _getRecordWriter(jobConf);
    }

    public RecordWriter<K, V> getRecordWriter(Properties jobConf) throws IOException {
        return _getRecordWriter(jobConf);
    }

    private <Config> RecordWriter<K, V> _getRecordWriter(Config jobConf) throws IOException {
        GraphiteConfiguration graphiteConf = new GraphiteConfiguration( jobConf );

        Socket graphiteSocket = new Socket(graphiteConf.getHost(), graphiteConf.getPort());
        PrintWriter pw = new PrintWriter(graphiteSocket.getOutputStream(), true);

        return new GraphiteRecordWriter(pw, graphiteConf.getRoot(), graphiteConf.getBufferSize(), graphiteConf.getTriggerSize());
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }
}
