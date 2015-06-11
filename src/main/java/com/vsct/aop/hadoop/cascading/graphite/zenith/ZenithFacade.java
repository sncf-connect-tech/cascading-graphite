package com.vsct.aop.hadoop.cascading.graphite.zenith;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;

/**
 * VSCT Graphite Key Normalization facade
 * Created by guillaume_dufour on 26/02/15.
 */
public class ZenithFacade {

    static final ZenithGraphiteTapFactory zgtf = new ZenithGraphiteTapFactory();

    public static String GRAPHITE_KEY_FIELD = "graphiteKey";
    public static String GRAPHITE_VALUE_FIELD = "graphiteValue";
    public static String GRAPHITE_TS_FIELD = "graphiteTs";

    public static ZenithGraphiteTapFactory getTapFactory() {
        return zgtf;
    }

    public static Pipe getGraphiteFormatedPipe(Pipe previous, ZenithGraphiteKeyBuilder builder, GRAPHITE_FREQUENCE frequency, GRAPHITE_FONCTION fonction, GRAPHITE_METRIC metric, String valueNameInCascading, String tsNameInCascading) {
        Pipe assembly = previous;
        ZenithGraphiteKeyFunction graphiteKeyFunction = builder.build(new Fields(GRAPHITE_KEY_FIELD), frequency, fonction, metric);

        assembly = new Each(assembly, Fields.ALL, graphiteKeyFunction, Fields.ALL);
        assembly = new Rename(assembly, new Fields(valueNameInCascading), new Fields(GRAPHITE_VALUE_FIELD));
        assembly = new Rename(assembly, new Fields(tsNameInCascading), new Fields(GRAPHITE_TS_FIELD));
        assembly = new Retain(assembly, new Fields(GRAPHITE_KEY_FIELD, GRAPHITE_VALUE_FIELD, GRAPHITE_TS_FIELD));
        return assembly;
    }

    public enum GRAPHITE_FREQUENCE {
        ONE_MIN("1min"), TEN_MIN("10min"), FIFT_MIN("15min"), ONE_HOUR("1hour"), ONE_MONTH("1month");
        private String value;

        GRAPHITE_FREQUENCE(String value) {
            this.value = value;
        }

        public String getFrequence() {
            return this.value;
        }
    }


    public enum GRAPHITE_FONCTION {
        COUNT("count"), RATE("rate"), MEAN("mean"), MAX("max"), MIN("min"), STDDEV("stddev"), P90("p90");
        private String value;

        private GRAPHITE_FONCTION(String value) {
            this.value = value;
        }

        public String getFonction() {
            return value;
        }
    }

    public enum GRAPHITE_METRIC {
        VOLUME("vol"), ERROR("error"), PERF("perf");
        private String value;

        private GRAPHITE_METRIC(String value) {
            this.value = value;
        }

        public String getMetric() {
            return value;
        }
    }

    public interface InstanceToSite {
        public String toSite(String instance);
    }

}
