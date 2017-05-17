//package com.ge.current.em.aggregation.broadcast;
//
//import com.ge.current.em.analytics.common.APMDataLoader;
//import com.ge.current.em.analytics.dto.PointObject;
//import com.ge.current.ie.broadcast.RefreshableBroadcast;
//import org.apache.log4j.Logger;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import java.io.Serializable;
//import java.util.Map;
//
///**
// * Created by 212582112 on 2/16/17.
// */
//public class NormalizationBroadcastManager implements Serializable {
//    private static final long serialVersionUID = 1L;
//
//    private static final Logger LOGGER = Logger.getLogger(NormalizationBroadcastManager.class);
//
//    private static RefreshableBroadcast<Map<String, String>> gatewaySiteMapping;
//    private static RefreshableBroadcast<Map<String, String>> assetEnterpriseMapping;
//    private static RefreshableBroadcast<Map<String, Map<String, PointObject>>> siteHaystackMapping;
//
//    public static void doBroadcast(JavaStreamingContext jsc,
//                                   APMDataLoader apmDataLoader) {
//        // @formatter: off
//        assetEnterpriseMapping = new RefreshableBroadcast<Map<String, String>>(jsc.sparkContext().broadcast(apmDataLoader.generateAssetEnterpriseMapping()),
//                                                                               sqlContext -> apmDataLoader.generateAssetEnterpriseMapping());
//        LOGGER.info("Asset Enterprise Mapping: " + assetEnterpriseMapping.getValue());
//
//        gatewaySiteMapping = new RefreshableBroadcast<Map<String, String>>(jsc.sparkContext().broadcast(apmDataLoader.getGatewaySiteMapping()),
//                                                                           sqlContext -> apmDataLoader.getGatewaySiteMapping());
//        LOGGER.info("Gateway Site Mapping: " + gatewaySiteMapping.getValue());
//
//        siteHaystackMapping = new RefreshableBroadcast<Map<String, Map<String, PointObject>>>(jsc.sparkContext().broadcast(apmDataLoader.getSiteHaystackMapping()),
//                                                                                              sqlContext -> apmDataLoader.getSiteHaystackMapping());
//        LOGGER.info("Site Haystack Mapping: " + siteHaystackMapping.getValue());
//        // @formatter: on
//    }
//
//    public static void refreshAll(SQLContext sqlContext, Map<String, String> options) {
//        gatewaySiteMapping.refreshBroadcast(sqlContext, options, true);
//        assetEnterpriseMapping.refreshBroadcast(sqlContext, options, true);
//        siteHaystackMapping.refreshBroadcast(sqlContext, options, true);
//    }
//
//    public static Broadcast<Map<String, String>> getGatewaySiteMapping() {
//        return gatewaySiteMapping.getBroadcast();
//    }
//
//    public static Broadcast<Map<String, String>> getAssetEnterpriseMapping() {
//        return assetEnterpriseMapping.getBroadcast();
//    }
//
//    public static Broadcast<Map<String, Map<String, PointObject>>> getSiteHaystackMapping() { return siteHaystackMapping.getBroadcast(); }
//}
