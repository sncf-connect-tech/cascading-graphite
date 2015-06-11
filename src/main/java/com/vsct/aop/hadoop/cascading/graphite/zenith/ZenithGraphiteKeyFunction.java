package com.vsct.aop.hadoop.cascading.graphite.zenith;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Arrays;

/**
 * Created by guillaume_dufour on 23/02/15.
 */
public class ZenithGraphiteKeyFunction extends BaseOperation implements Function {

    //VSL, VMO etc.
    private String client;
    //itineraries, AQ, etc.
    private String service;
    //getOutward, any, etc.
    private String methode;

    //VSL, MBP etc.
    private String entryPoint;
    //PRD, INT etc
    private String platform;

    private String platformNumber;
    //SIDH
    private String asset;
    private String trigramme;
    //JAVA, HTTP...
    private String system;
    //
    private String assetInterface;
    //IDHNEM11 etc
    private String instance;
    //application io or third party io logs
    private String pointCut;
    //log type
    private String logFileOrPartner;
    //sous service (for third-party)
    private String extService;
    //vol/perf/error
    private ZenithFacade.GRAPHITE_METRIC metric;


    //frequence= 1min, 10min, 15min, 1hour, 1day, 1month, etc.1min, 10min, 15min, 1hour, 1day, 1month, etc
    private ZenithFacade.GRAPHITE_FREQUENCE frequency;
    // count, rate, mean, max/min, stddev, p90.
    private ZenithFacade.GRAPHITE_FONCTION fonction;

    //error code, request type etc.
    private String focus;

    private ZenithFacade.InstanceToSite i2s;

    public ZenithGraphiteKeyFunction(Fields graphiteKeyCol, String entryPoint, String trigramme, String asset, String platform, String platformNumber,
                                     String system, String instance, String assetInterface, String client, String service, String methode,
                                     String focus, String pointCut, String logFileOrPartner, String extService,
                                     ZenithFacade.GRAPHITE_FREQUENCE frequency, ZenithFacade.GRAPHITE_FONCTION fonction, ZenithFacade.GRAPHITE_METRIC metric, ZenithFacade.InstanceToSite i2s) {
        super(1,graphiteKeyCol);
        this.asset=asset;
        this.trigramme=trigramme;
        this.system=system;
        this.assetInterface=assetInterface;
        this.pointCut=pointCut;
        this.logFileOrPartner=logFileOrPartner;
        this.extService = extService;
        this.client = client;
        this.service = service;
        this.methode = methode;
        this.entryPoint = entryPoint;
        this.platform = platform;
        this.instance = instance;
        this.metric = metric;
        this.frequency = frequency;
        this.fonction = fonction;
        this.platformNumber = platformNumber;
        this.focus = focus;
        this.i2s = i2s;
    }

    private String getStringOrAny(TupleEntry arguments, String column){
        String value = "any";
        if(column != null) {
            if(arguments.getFields().contains(new Fields(column)))
                value = arguments.getString(column);
            else
                value = column;
        }
        return value;
    }
    
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        Tuple result = new Tuple();
        TupleEntry arguments = functionCall.getArguments();

        String entryPointName = getStringOrAny(arguments, entryPoint);
        String trigrammeName = getStringOrAny(arguments, trigramme);
        String assetName = getStringOrAny(arguments, asset);
        String assetInterfaceName = getStringOrAny(arguments, assetInterface);

        String instanceName = getStringOrAny(arguments, instance);
        String site = "any";
        if(i2s != null) {
            site = i2s.toSite(instanceName);
        }

        String platformName = "any";
        if(platform != null && platformNumber != null) {
            platformName = getStringOrAny(arguments, platform)+getStringOrAny(arguments, platformNumber);
        }

        String systemName = getStringOrAny(arguments, system);
        String clientName = getStringOrAny(arguments, client);
        String serviceName = getStringOrAny(arguments, service);
        String methodeName = getStringOrAny(arguments, methode);

        String pointCutName = getStringOrAny(arguments, pointCut);
        String logFileOrPartnerName = getStringOrAny(arguments, logFileOrPartner);
        String extServiceName = getStringOrAny(arguments, extService);

        String focusName = getStringOrAny(arguments, focus).toLowerCase();

        result.addString(entryPointName+"."+trigrammeName+"."+assetName+"."+site+"."+platformName+"."+systemName+"."+instanceName+"."+assetInterfaceName
                +"."+clientName+"."+serviceName+"."+methodeName+"."+pointCutName+"."+logFileOrPartnerName+"."+extServiceName+"."+this.metric.getMetric()+"."
                +focusName+"."+this.frequency.getFrequence()+"."+this.fonction.getFonction());
        functionCall.getOutputCollector().add( result );
    }

}
