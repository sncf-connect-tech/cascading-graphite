package com.vsct.aop.hadoop.cascading.graphite.zenith;

import cascading.tuple.Fields;

/**
 * Created by guillaume_dufour on 23/02/15.
 */
public class ZenithGraphiteKeyBuilder {

    
    private String client = null;
    private String service = null;
    private String methode = null;
    private String entryPoint = null;
    private String platform = null;
    private String platformNumber = null;
    private String asset = null;
    private String trigramme = null;
    private String system = null;
    
    // 0_0, 20_0, ... or any
    private String assetInterface = null;
    
    //IDHNEM11 etc
    private String instance = null;
    private String pointCut = null;
    
    private String logFileOrPartner = null;
    
    //sous service (for third-party)
    private String extService = null;
    
    private String focus = null;

    private ZenithFacade.InstanceToSite i2s = null;


    /**
     * @param graphiteKeyField the column name of the key
     * @param frequency time frequency 10min, 1hour, ...
     * @param fonction function like count, rate, min, ...
     * @param metric metric name like vol, err, perf
     * @return build the key
     */
    public ZenithGraphiteKeyFunction build(Fields graphiteKeyField, ZenithFacade.GRAPHITE_FREQUENCE frequency, ZenithFacade.GRAPHITE_FONCTION fonction, ZenithFacade.GRAPHITE_METRIC metric) {
        return new ZenithGraphiteKeyFunction(graphiteKeyField, entryPoint, trigramme, asset, platform, platformNumber,
                system, instance, assetInterface, client, service, methode,
                focus, pointCut, logFileOrPartner, extService,
                frequency, fonction, metric, i2s);

    }

    /**
     * @param client VSL, VMO etc.
     * @return this
     */
    public ZenithGraphiteKeyBuilder setClient(String client) {
        this.client = client;
        return this;
    }

    /**
     * @param service AQ etc.
     * @return this
     */
    public ZenithGraphiteKeyBuilder setService(String service) {
        this.service = service;
        return this;
    }

    /**
     * @param methode getOutwardStatless etc.
     * @return this
     */
    public ZenithGraphiteKeyBuilder setMethode(String methode) {
        this.methode = methode;
        return this;
    }

    /**
     * @param entryPoint VSL, MBP etc.
     * @return this
     */
    public ZenithGraphiteKeyBuilder setEntryPoint(String entryPoint) {
        this.entryPoint = entryPoint;
        return this;
    }

    /**
     * @param platform PRD, INT, REC etc
     * @return this
     */
    public ZenithGraphiteKeyBuilder setPlatform(String platform) {
        this.platform = platform;
        return this;
    }

    /**
     * @param platformNumber 1, 2, 3, ... , 7, ... etc
     * @return this
     */
    public ZenithGraphiteKeyBuilder setPlatformNumber(String platformNumber) {
        this.platformNumber = platformNumber;
        return this;
    }

    /**
     * @param asset SIDH, WDI, iDTGV, OuiGO, ...
     * @return this
     */
    public ZenithGraphiteKeyBuilder setAsset(String asset) {
        this.asset = asset;
        return this;
    }

    /**
     * @param trigramme IDH, WDI, NDS, LOW, ...
     * @return this
     */
    public ZenithGraphiteKeyBuilder setTrigramme(String trigramme) {
        this.trigramme = trigramme;
        return this;
    }

    /**
     * @param system WAS, HAP...
     * @return this
     */
    public ZenithGraphiteKeyBuilder setSystem(String system) {
        this.system = system;
        return this;
    }

    /**
     * @param assetInterface if necessary, like 0_0
     * @return this
     */
    public ZenithGraphiteKeyBuilder setAssetInterface(String assetInterface) {
        this.assetInterface = assetInterface;
        return this;
    }

    /**
     * @param instance name like VSLMONP71WDI
     * @return this
     */
    public ZenithGraphiteKeyBuilder setInstance(String instance) {
        this.instance = instance;
        return this;
    }

    /**
     * @param pointCut io or part
     * @return this
     */
    public ZenithGraphiteKeyBuilder setPointCut(String pointCut) {
        this.pointCut = pointCut;
        return this;
    }

    /**
     * @param logFileOrPartner log type or partner
     * @return this
     */
    public ZenithGraphiteKeyBuilder setLogFileOrPartner(String logFileOrPartner) {
        this.logFileOrPartner = logFileOrPartner;
        return this;
    }

    /**
     * @param extService external (partner) services
     * @return this
     */
    public ZenithGraphiteKeyBuilder setExtService(String extService) {
        this.extService = extService;
        return this;
    }

    /**
     * @param focus error code, request type etc.
     * @return this
     */
    public ZenithGraphiteKeyBuilder setFocus(String focus) {
        this.focus = focus;
        return this;
    }

    /**
     * @param i2s instance name to site name
     * @return this
     */
    public ZenithGraphiteKeyBuilder setI2s(ZenithFacade.InstanceToSite i2s) {
        this.i2s = i2s;
        return this;
    }

}
