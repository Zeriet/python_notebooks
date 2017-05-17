package com.ge.current.ie.analytics.batch;

import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_CONNECTION_PORT;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.ie.queueutil.IEAmqpUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.ge.current.em.aggregation.dao.OutOfSyncLog;
import com.ge.current.em.aggregation.utils.DateUtils;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;

import scala.Tuple2;

@Component
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class OutOfSyncBatch implements ISparkBatch, Serializable {

    @Autowired
    PropertiesBean propertiesBean;

    @Autowired
    transient APMDataLoader apmDataLoader;

    Broadcast<Map<String, Set<String>>> assetSegmentMappingBC = null;
    Broadcast<Map<String, String>> assetSiteMappingBC = null;
    Broadcast<Map<String, String>> assetEnterpriseMappingBC = null;
    Broadcast<Map<String, String>> assetTypeMappingBC = null;
    private static Broadcast<Map<String, String>> siteEnterpriseMappingBC = null;
    private static Broadcast<Map<String, String>> segmentEnterpriseMappingBC = null;
    private static Broadcast<Map<String, List<String>>> subMeterToSegmentsMappingBC = null;

    private Broadcast<Map<String, Set<String>>> assetSubmeterMappingBC = null;
    private Broadcast<Map<String, Set<String>>> segmentSubmeterMappingBC = null;
    private Broadcast<Map<String, Set<String>>> siteSubmeterMappingBC = null;
    private Broadcast<Map<String, Set<String>>> enterpriseSubmeterMappingBC = null;

    private Broadcast<Map<String, String>> segmentLoadTypeMappingBC = null;

    @Override
    public void broadcast(SparkContext sc, PropertiesBean propertiesBean) {
        System.out.println("**** Sending Broadcast assetSegmentMappingBC ****");
        Map<String, String> assetEnterpriseMapping = apmDataLoader.generateAssetEnterpriseMapping();
        Map<String, String> assetSiteMapping = apmDataLoader.generateAssetSiteMapping();
        Map<String, String> siteEnterpriseMapping = apmDataLoader.generateSiteEnterpriseMapping();
        Map<String, String> segmentEnterpriseMapping = apmDataLoader.generateSegmentEnterpriseMapping();
        Map<String, Set<String>> assetSegmentMapping = apmDataLoader.generateAssetSegmentMapping();

        Map<String, Set<String>> assetSubmeterMapping = apmDataLoader.generateAssetSegmentMapping();
        Map<String, Set<String>> segmentSubmeterMapping = apmDataLoader.getSegmentToSubmeterMapping();
        Map<String, Set<String>> siteSubmeterMapping = apmDataLoader.getSiteToSubmeterMapping();
        Map<String, Set<String>> enterpriseSubmeterMapping = apmDataLoader.getEnterpriseToSubmeterMapping();
        Map<String, String> segmentLoadTypeMapping = apmDataLoader.getLoadTypeMapping();

        assetSegmentMappingBC = sc.broadcast(assetSegmentMapping,
                scala.reflect.ClassTag$.MODULE$.apply(assetSegmentMapping.getClass()));
        assetEnterpriseMappingBC = sc.broadcast(assetEnterpriseMapping,
                scala.reflect.ClassTag$.MODULE$.apply(assetEnterpriseMapping.getClass()));
        assetSiteMappingBC = sc.broadcast(assetSiteMapping,
                scala.reflect.ClassTag$.MODULE$.apply(assetSiteMapping.getClass()));
        siteEnterpriseMappingBC = sc.broadcast(siteEnterpriseMapping,
                scala.reflect.ClassTag$.MODULE$.apply(siteEnterpriseMapping.getClass()));
        segmentEnterpriseMappingBC = sc.broadcast(segmentEnterpriseMapping,
                scala.reflect.ClassTag$.MODULE$.apply(segmentEnterpriseMapping.getClass()));

        assetSubmeterMappingBC = sc.broadcast(assetSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(assetSubmeterMapping.getClass()));
        segmentSubmeterMappingBC = sc.broadcast(segmentSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(segmentSubmeterMapping.getClass()));
        siteSubmeterMappingBC = sc.broadcast(siteSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(siteSubmeterMapping.getClass()));
        enterpriseSubmeterMappingBC = sc.broadcast(enterpriseSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(enterpriseSubmeterMapping.getClass()));
        segmentLoadTypeMappingBC = sc.broadcast(segmentLoadTypeMapping, scala.reflect.ClassTag$.MODULE$.apply(segmentLoadTypeMapping.getClass()));

    }


    @Override
    public void run(Environment env, String startTime, String endTime, String timeZone) {

        SparkConf sparkConf = new SparkConf().setMaster(propertiesBean.getValue(SparkConfigurations.SPARK_MASTER))
                .setAppName("Out_Of_Sync_Batch");

        sparkConf.set("spark.cassandra.auth.username", "ge_current");
        sparkConf.set("spark.cassandra.auth.password", "c5V7DdOUnb6OXfDGGqlk");
        sparkConf.set("spark.cassandra.connection.host",
                getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_CONNECTION_HOST));
        sparkConf.set("spark.cassandra.connection.port", getPropertiesBean().getValue(CASSANDRA_CONNECTION_PORT));

        SparkContext sc = new SparkContext(sparkConf);
        broadcast(sc, getPropertiesBean());
        CassandraSQLContext cassandraSQLContext = new CassandraSQLContext(sc);
        process(sc, cassandraSQLContext, startTime);
    }

    private void process(SparkContext sc, CassandraSQLContext cassandraSQLContext, String timeBucket) {

        String keySpace = propertiesBean.getValue(SparkConfigurations.CASSANDRA_KEYSPACE);
        String tableName = keySpace + ".out_of_sync_log";
        timeBucket = timeBucket.substring(0, 10);
        System.out.println("TIME BUCKET : " + timeBucket);
        String query = "Select yyyymmddhhmm, enterprise_uid, resrc_uid, event_type, time_bucket, resrc_tz from %s where time_bucket = '%s'";
        query = String.format(query, tableName, timeBucket);

        DataFrame dataFrame = cassandraSQLContext.cassandraSql(query).cache();

        if (dataFrame.count() > 0) {

            System.out.println("++++++++++++++++++++++++++++++++++++++++");
            System.out.println("..... Start Processing 15 MIN Job ......");
            System.out.println("++++++++++++++++++++++++++++++++++++++++");

            JavaRDD<Row> rows = dataFrame.toJavaRDD().cache();

            System.out.println("Count of Rows " + rows.count());

            JavaRDD<OutOfSyncLog> outOfSyns = rows.map(new Function<Row, OutOfSyncLog>() {

                @Override
                public OutOfSyncLog call(Row row) throws Exception {
                    String yyyyMMddHHmm = row.getString(0);

                    OutOfSyncLog log = new OutOfSyncLog();
                    log.setEnterprise_uid(row.getString(1));
                    log.setEvent_type(row.getString(3));
                    log.setResrc_uid(row.getString(2));
                    log.setYyyymmddhhmm(row.getString(0));
                    log.setTime_bucket(row.getString(4));
                    log.setResrc_tz(row.getString(5));
                    if (log.getResrc_type() == null) {
                        log.setResrc_type("ASSET");
                    }
                    switch (yyyyMMddHHmm.length()) {
                        case 12:
                            log.runForMin = true;
                        case 10:
                            log.runForHour = true;
                        case 8:
                            log.runForDay = true;
                        case 6:
                            log.runForMonth = true;
                    }
                    return log;
                }
            }).filter(oos -> oos != null);

            outOfSyns.cache();

            processes15Min(sc, cassandraSQLContext, keySpace, outOfSyns);

            System.out.println("++++++++++++++++++++++++++++++++++++++++");
            System.out.println("..... Start Processing Hourly Job ......");
            System.out.println("++++++++++++++++++++++++++++++++++++++++");


            JavaRDD<OutOfSyncLog> fullOutOfSyns = outOfSyns.flatMap(new FlatMapFunction<OutOfSyncLog, OutOfSyncLog>() {

                @Override
                public Iterable<OutOfSyncLog> call(OutOfSyncLog oos) throws Exception {
                    List<OutOfSyncLog> list = new ArrayList<>();
                    list.add(oos);
                    Set<String> segments = assetSegmentMappingBC.getValue().get(oos.resrc_uid);
                    if (segments != null) {
                        for (String segId : segments) {
                            OutOfSyncLog newOOS = new OutOfSyncLog();
                            BeanUtils.copyProperties(newOOS, oos);
                            newOOS.setResrc_type("SEGMENT");
                            newOOS.setResrc_uid(segId);
                            list.add(newOOS);
                        }
                    }
                    String enterprise = assetEnterpriseMappingBC.getValue().get(oos.resrc_uid);
                    if (null != enterprise) {
                        OutOfSyncLog entOOS = new OutOfSyncLog();
                        BeanUtils.copyProperties(entOOS, oos);
                        entOOS.setResrc_type("ENTERPRISE");
                        entOOS.setResrc_uid(enterprise);
                        list.add(entOOS);
                    }
                    String site = assetSiteMappingBC.getValue().get(oos.resrc_uid);
                    if (null != site) {
                        OutOfSyncLog siteOOS = new OutOfSyncLog();
                        BeanUtils.copyProperties(siteOOS, oos);
                        siteOOS.setResrc_type("SITE");
                        siteOOS.setResrc_uid(site);
                        list.add(siteOOS);
                    }
                    return list;
                }

            });

            processesHourly(sc, cassandraSQLContext, keySpace, fullOutOfSyns);

            System.out.println("++++++++++++++++++++++++++++++++++++++++");
            System.out.println("..... Start Processing Daily Job ......");
            System.out.println("++++++++++++++++++++++++++++++++++++++++");

            processesDaily(sc, cassandraSQLContext, keySpace, fullOutOfSyns);

            System.out.println("++++++++++++++++++++++++++++++++++++++++");
            System.out.println("..... Start Processing Monthly Job ......");
            System.out.println("++++++++++++++++++++++++++++++++++++++++");

            processesMonthly(sc, cassandraSQLContext, keySpace, fullOutOfSyns);

            System.out.println("++++++++++++++++++++++++++++++++++++++++");
            System.out.println("..... CLOSING Rabbit MQ Connection ......" + new Date());
            System.out.println("++++++++++++++++++++++++++++++++++++++++");

            //close the channel
            try {
                IEAmqpUtil.closeConnection(false);
            } catch (Exception e) {
                LOG.error("Failed to close the AMQP connection.", e);
                LOG.error("Closing the connection forcefully");
                try {
                    IEAmqpUtil.closeConnection(true);
                } catch (Exception e1) {
                    LOG.error("CLosing the connection forcefully", e1);
                }
            }

            System.out.println("++++++++++++++++++++++++++++++++++++++++");
            System.out.println("..... COMPLETED ......" + new Date());
            System.out.println("++++++++++++++++++++++++++++++++++++++++");
        }
    }

    private void processesMonthly(SparkContext sc, CassandraSQLContext cassandraSQLContext,
                                  String keySpace, JavaRDD<OutOfSyncLog> outOfSyns) {
        // Fix Monthly Records

        List<Tuple2<String, OutOfSyncLog>> list = outOfSyns
                .filter(o -> {
                    return o.isRunForMonth();
                })
                .mapToPair(new PairFunction<OutOfSyncLog, String, OutOfSyncLog>() {
                    @Override
                    public Tuple2<String, OutOfSyncLog> call(OutOfSyncLog log) throws Exception {
                        String tb = log.yyyymmddhhmm;
                        // Just Get Day from timeBucket;
                        tb.substring(0, 6);
                        return new Tuple2<String, OutOfSyncLog>(tb.substring(0, 6) + log.resrc_uid, log);
                    }
                })
                .reduceByKey(new Function2<OutOfSyncLog, OutOfSyncLog, OutOfSyncLog>() {

                    @Override
                    public OutOfSyncLog call(OutOfSyncLog v1, OutOfSyncLog v2) throws Exception {
                        return v1;
                    }
                })
                .collect();

        //.parallelStream()
        for (Tuple2<String, OutOfSyncLog> oos : list) {
            //(oos -> {
            OutOfSyncLog os = oos._2;
            Date localTime = null;
            try {
                localTime = DateUtils.convertToUTC(os.yyyymmddhhmm.substring(0, 6), "yyyyMM");
            } catch (Exception e) {
                e.printStackTrace();
            }
//			String localTimeString = DateUtils.convertUTCDateToLocalString(localTime, os.resrc_tz, "yyyyMM");
//			SparkCassandraMonthlyBatch monthlyBatch = new SparkCassandraMonthlyBatch();
//			String eventBucket = os.resrc_tz + "." + os.resrc_type + "." + os.resrc_uid + "." + os.event_type;
//			String dailyQuery = "SELECT resrc_uid, measures_aggr, measures_avg,  measures_cnt,measures_max, measures_min,tags, enterprise_uid, resrc_type FROM %s  WHERE event_bucket= '%s' AND enterprise_uid='%s'  AND yyyymm='%s'  ";
//			String eventsDailyTableName = keySpace + ".events_daily";
//			dailyQuery = String.format(dailyQuery, eventsDailyTableName, eventBucket, os.enterprise_uid,
//					localTimeString);
//			System.out.println(" Events Daily Query >>. " + dailyQuery);
//			DataFrame dailyDataFrame = cassandraSQLContext.cassandraSql(dailyQuery);
//			monthlyBatch.processMonthly(sc, localTimeString, dailyDataFrame, os.resrc_tz, keySpace);
        }
        //);
    }

    private void processesDaily(SparkContext sc, CassandraSQLContext cassandraSQLContext,
                                String keySpace, JavaRDD<OutOfSyncLog> outOfSyns) {
        // Fix Day Records

        List<Tuple2<String, OutOfSyncLog>> list = outOfSyns.filter(o -> {
            return o.isRunForDay();
        }).mapToPair(new PairFunction<OutOfSyncLog, String, OutOfSyncLog>() {
            @Override
            public Tuple2<String, OutOfSyncLog> call(OutOfSyncLog log) throws Exception {
                String tb = log.yyyymmddhhmm;
                // Just Get Day from timeBucket;
                tb.substring(0, 8);
                return new Tuple2<String, OutOfSyncLog>(tb.substring(0, 8) + log.resrc_uid, log);
            }
        }).reduceByKey(new Function2<OutOfSyncLog, OutOfSyncLog, OutOfSyncLog>() {
            @Override
            public OutOfSyncLog call(OutOfSyncLog v1, OutOfSyncLog v2) throws Exception {
                return v1;
            }
        }).collect();

        for (Tuple2<String, OutOfSyncLog> tuple : list) {
            //		.forEach(tuple -> {
            OutOfSyncLog oos = tuple._2;
            Date localTime = null;
            try {
                localTime = DateUtils.convertToUTC(oos.yyyymmddhhmm.substring(0, 8), "yyyyMMdd");
            } catch (Exception e) {
                e.printStackTrace();
            }

//			String localTimeString = DateUtils.convertUTCDateToLocalString(localTime, oos.resrc_tz, "yyyyMMdd");
//			String hourlyQuery = "select resrc_uid, measures_aggr, measures_avg, measures_cnt, measures_max, measures_min, tags, enterprise_uid, resrc_type from %s where event_bucket = '%s' and enterprise_uid = '%s' and yyyymmdd = '%s'";
//			try {
//				// PROCESS FOR ASSET
//				String hourlyTableName = keySpace + ".events_hourly";
//				String eventBucket = oos.resrc_tz + "." + oos.resrc_type + "." + oos.resrc_uid + "." + oos.event_type;
//				String assetHourlyQuery = String.format(hourlyQuery, hourlyTableName, eventBucket, oos.enterprise_uid,
//						localTimeString);
//				System.out.println(" Asset Hourly Query ..." + assetHourlyQuery);
//				DataFrame assetDataFrame = cassandraSQLContext.cassandraSql(assetHourlyQuery);
//				if (assetDataFrame.count() > 0) {
//					SparkCassandraDailyBatch.aggregateDailyEvents(sc, keySpace, localTimeString, assetDataFrame,
//							oos.resrc_tz);
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
        }
        //);
    }

    private void processesHourly(SparkContext sc, CassandraSQLContext cassandraSQLContext,
                                 String keySpace, JavaRDD<OutOfSyncLog> outOfSyns) {

        List<Tuple2<String, OutOfSyncLog>> list = outOfSyns.filter(o -> {
            return o.isRunForHour();
        }).mapToPair(log -> {
            return new Tuple2<String, OutOfSyncLog>(log.yyyymmddhhmm.substring(0, 10) + log.resrc_uid + log.event_type, log);
        }).reduceByKey(new Function2<OutOfSyncLog, OutOfSyncLog, OutOfSyncLog>() {

            @Override
            public OutOfSyncLog call(OutOfSyncLog v1, OutOfSyncLog v2) throws Exception {
                return v1;
            }
        }).collect();

        for (Tuple2<String, OutOfSyncLog> tuple : list) {
            //(tuple -> {

            OutOfSyncLog oos = tuple._2;
            String normalizedTable = keySpace + ".events_byminute";
            String eventBucketHourly = oos.resrc_tz + "." + oos.resrc_type + "." + oos.resrc_uid + "." + oos.event_type;
            Date localTime = null;
            try {
                localTime = DateUtils.convertToUTC(oos.yyyymmddhhmm.substring(0, 10), "yyyyMMddHH");
            } catch (Exception e) {
                e.printStackTrace();
            }
//			String localTimeString = DateUtils.convertUTCDateToLocalString(localTime, oos.resrc_tz, "yyyyMMddHH");
//			String minuteQuery = "select resrc_uid, measures_aggr, measures_avg, measures_cnt, measures_max, measures_min, tags, enterprise_uid, resrc_type from %s where  event_bucket = '%s' and enterprise_uid = '%s' and yyyymmdd = '%s' ";
//			minuteQuery = String.format(minuteQuery, normalizedTable, eventBucketHourly, oos.enterprise_uid,
//					localTimeString);
//			System.out.println("Events By Minute Query : " + minuteQuery);
//			DataFrame hourlyDataFrame = cassandraSQLContext.cassandraSql(minuteQuery).cache();
//			if (hourlyDataFrame.count() > 0) {
//				SparkCassandraHourlyBatch hourlyBatch = new SparkCassandraHourlyBatch();
//				hourlyBatch.aggregateHourlyEventsFrom15Mins(sc, keySpace, localTimeString, oos.resrc_tz, hourlyDataFrame);
//			}
        }
        //);
    }

    private void processes15Min(final SparkContext sc, CassandraSQLContext cassandraSQLContext, String keySpace,
                                JavaRDD<OutOfSyncLog> outOfSyns) {

        List<OutOfSyncLog> minEvents = outOfSyns.collect();

        //minEvents.stream()

        for (OutOfSyncLog oos : minEvents) {
            //.forEach(oos -> {
            System.out.println(oos.toString());
            if (oos.isRunForMin()) {
                System.out.println("Going to Recalculate 15 Min Record for Time-Bucket " + oos.yyyymmddhhmm + ", Asset : "
                        + oos.resrc_uid);
                String normalizedTable = keySpace + ".event_norm_log";
                String normalizationQuery = "select resrc_uid, measures, tags, site_uid, enterprise_uid, event_ts, event_ts_tz from %s where  time_bucket = '%s' and resrc_uid = '%s' and event_type = '%s' ";
                normalizationQuery = String.format(normalizationQuery, normalizedTable, oos.yyyymmddhhmm, oos.resrc_uid,
                        oos.event_type);
                System.out.println("Normalized Query : " + normalizationQuery);
                DataFrame normDataFrame = cassandraSQLContext.cassandraSql(normalizationQuery);
                if (normDataFrame.count() > 0) {
                    int assetUidIndex = 0;
                    int siteUidIndex = 3;
                    int enterpriseUidIndex = 4;
//					try {
//						SparkCassandra15MinsBatch minBatch = new SparkCassandra15MinsBatch();
//						minBatch.setMetaData(assetSiteMappingBC, assetEnterpriseMappingBC,
//								siteEnterpriseMappingBC, segmentEnterpriseMappingBC, assetSegmentMappingBC
//								,assetSubmeterMappingBC,segmentSubmeterMappingBC,siteSubmeterMappingBC,enterpriseSubmeterMappingBC,segmentLoadTypeMappingBC, propertiesBean);
//						minBatch.process15MinAggregations(sc, normDataFrame, keySpace, oos.yyyymmddhhmm, assetUidIndex,
//								siteUidIndex, enterpriseUidIndex, oos.resrc_tz);
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
                }
            }
        }
        //);
    }

    @Override
    public PropertiesBean getPropertiesBean() {

        return propertiesBean;
    }

    @Override
    public String getMaster() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getAppName() {
        // TODO Auto-generated method stub
        return "Out OF Sync Batch";
    }

}