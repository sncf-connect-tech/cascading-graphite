package com.vsct.aop.hadoop.cascading.graphite.zenith;

import cascading.tap.Tap;
import com.vsct.aop.hadoop.cascading.graphite.GraphiteTapFactory;

import java.io.IOException;

/**
 * Created by guillaume_dufour on 26/02/15.
 */
public class ZenithGraphiteTapFactory extends GraphiteTapFactory {

    public Tap createTap(String graphiteKeyField, String valueField, String timestampField, String graphiteHost, int graphitePort) throws IOException {
        return super.createTap(graphiteKeyField, valueField, timestampField, graphiteHost, graphitePort, "Zenith");
    }


}
