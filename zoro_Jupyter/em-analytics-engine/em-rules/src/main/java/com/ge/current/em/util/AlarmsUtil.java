package com.ge.current.em.util;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.entities.analytics.AlarmObject;
import com.ge.current.em.entities.analytics.AlertLog;
import com.ge.current.em.entities.analytics.AlertLogStg;
import javafx.scene.control.Alert;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by 212565238 on 2/24/17.
 */
@Component
public class AlarmsUtil implements Serializable {

    static final Logger LOG = LoggerFactory.getLogger(AlarmsUtil.class);

    public static JavaRDD<AlertLog> getAllTheAlarmsToBeSaved(JavaRDD<AlertLog> inputAlarmObject, JavaRDD<AlertLog> pastAlertLog, String currentTimeBucket, String pastTimeBucket, List<String> ruleNames, String timeZone) {
        JavaPairRDD<String, AlertLog> inputAlarmObjectPairRDD = getAlertLogJavaPairRDD(inputAlarmObject);
        LOG.warn("current alert count: {}", inputAlarmObjectPairRDD.count());
        LOG.warn("pastAlertLog alerts count: {}", pastAlertLog.count());
        JavaPairRDD<String, AlertLog> pastAlertLogPairRDD = getAlertLogJavaPairRDD(AlarmsUtil.filterAlertLogForRuleNames(ruleNames, pastAlertLog, timeZone));
        LOG.info("pastAlertLogPairRDD alerts count: {}", pastAlertLogPairRDD.rdd().count());


        JavaPairRDD<String, AlertLog> unionAlertLogPairRdd = inputAlarmObjectPairRDD.union(pastAlertLogPairRDD);
        LOG.info("unionAlertLogPairRdd alerts count: {}", unionAlertLogPairRdd.rdd().count());
        JavaPairRDD<String, Iterable<AlertLog>> groupByAlterLogPAirRDD = unionAlertLogPairRdd.groupByKey();

        LOG.info("groupByAlterLogPAirRDD alerts count: {}", groupByAlterLogPAirRDD.rdd().count());
        JavaRDD<AlertLog> aggregatedAlertLog = groupByAlterLogPAirRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<AlertLog>>, AlertLog>() {
            @Override
            public Iterable<AlertLog> call(Tuple2<String, Iterable<AlertLog>> alertsTuples) throws Exception {
                List<AlertLog> alertLogs = new ArrayList<>();
                Iterable<AlertLog> alerts = alertsTuples._2;
                int totalDuration = 0;
                int currentDuration = 0;
                int pastDuration = 0;
                AlertLog pastAlertLog = null;
                AlertLog currentAlertLog = null;
                for(AlertLog alertLog : alerts) {
                    //assuming that the previous time bucket always contains an active alarm as its been created in the last run
                    if(alertLog.getTime_bucket().equals(pastTimeBucket) && "active".equalsIgnoreCase(alertLog.getStatus())) {
                        pastDuration += alertLog.getDuration();
                        totalDuration += alertLog.getDuration();
                        pastAlertLog = alertLog;
                    } else  if(alertLog.getTime_bucket().equals(currentTimeBucket) && "active".equalsIgnoreCase(alertLog.getStatus())) {
                        currentDuration  += alertLog.getDuration();
                        totalDuration += alertLog.getDuration();
                        currentAlertLog = alertLog;
                    }
                }
                LOG.info("currentalertLog, pastAlertLog: {}, {}, pastTime: {} , currentTime: {}", currentAlertLog, pastAlertLog, pastTimeBucket, currentTimeBucket);
                if(currentAlertLog == null && pastAlertLog != null) {
                    LOG.info("pastAlert is null: {}, duration : {}, status: {}", pastAlertLog.getResrc_uid() ,pastAlertLog.getDuration(), pastAlertLog.getStatus());
                    pastAlertLog.setStatus("past");
                    pastAlertLog.setTime_bucket(pastTimeBucket);
                    alertLogs.add(pastAlertLog);
                    AlertLog inactiveAlertLog = new AlertLog(pastAlertLog);
                    inactiveAlertLog.setStatus("inactive");
                    inactiveAlertLog.setTime_bucket(currentTimeBucket);
                    LOG.info("new pastAlert is null: {}, duration : {}, status: {}", pastAlertLog.getResrc_uid() ,pastAlertLog.getDuration(), pastAlertLog.getStatus());
                    alertLogs.add(inactiveAlertLog);
                } else if(pastAlertLog == null  && currentAlertLog!= null) {
                    LOG.info("pastAlert is null: {}, duration : {}, status: {}", currentAlertLog.getResrc_uid() ,currentAlertLog.getDuration(), currentAlertLog.getStatus());
                    currentAlertLog.setStatus("active");
                    currentAlertLog.setTime_bucket(currentTimeBucket);
                    alertLogs.add(currentAlertLog);
                } else if(currentAlertLog!= null && pastAlertLog != null){
                    LOG.info("both present is not null: {}, duration : {}, status: {}", currentAlertLog.getResrc_uid() ,currentAlertLog.getDuration(), currentAlertLog.getStatus());

                    currentAlertLog.setDuration(totalDuration);
                    currentAlertLog.setTime_bucket(currentTimeBucket);
                    currentAlertLog.setStatus("active");
                    currentAlertLog.setAlert_ts(pastAlertLog.getAlert_ts());
                    currentAlertLog.setAlert_ts_loc(pastAlertLog.getAlert_ts_loc());
                    pastAlertLog.setStatus("past");
                    LOG.info("both past is not null: {}, duration : {}, status: {}", pastAlertLog.getResrc_uid() ,pastAlertLog.getDuration(), pastAlertLog.getStatus());
                    alertLogs.add(pastAlertLog);
                    alertLogs.add(currentAlertLog);
                }
                return alertLogs;
            }
        });

//        JavaRDD<AlertLog> aggregatedAlertLog = joinedRdd.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Optional<AlertLog>, AlertLog>>, AlertLog>() {
//            @Override
//            public Iterable<AlertLog> call(Tuple2<String, Tuple2<Optional<AlertLog>, AlertLog>> stringTuple2Tuple2) throws Exception {
//                AlertLog pastAlertLog = stringTuple2Tuple2._2()._2();
//                AlertLog currentAlertLog = stringTuple2Tuple2._2()._1;
//                LOG.info("pastAlertLog: {}, currentAlertLog: {} ", pastAlertLog, currentAlertLog);
//                List<AlertLog> alerts = new ArrayList<>();
//                if (stringTuple2Tuple2._2()._1() == null) {
//                    LOG.info("******** stringTuple2Tuple2._2()._1(): ");
//
//                    pastAlertLog.setStatus("Inactive");
//                    alerts.add(pastAlertLog);
//                } else if (stringTuple2Tuple2._2()._2() == null) {
//
//                    LOG.info("******** stringTuple2Tuple2._2()._2() : ");
//                    currentAlertLog.setStatus("inactive");
//                    alerts.add(currentAlertLog);
//                } else {
//                    LOG.info("******** else: ");
//                    currentAlertLog.setStatus("active");
//
//                    pastAlertLog.setStatus("Inactive");
//                    int pastDuration = pastAlertLog.getDuration();
//                    int currentDuration = currentAlertLog.getDuration();
//                    LOG.info("pastAlertLog resource_uid {} , currentAlertLog: {}  ", pastAlertLog.getResrc_uid(), currentAlertLog.getResrc_uid());
//                    LOG.info("pastDuration: {} , currentDuration: {} , pastDuration+currentDuration: {} ", pastDuration, currentDuration, pastDuration + currentDuration);
//                    currentAlertLog.setDuration(pastDuration + currentDuration);
//                    alerts.add(currentAlertLog);
//                    alerts.add(pastAlertLog);
//                }
//
//                return alerts;
//            }
//        });

        LOG.info("aggregatedAlertLog alerts count: {}", aggregatedAlertLog.count());
        return aggregatedAlertLog;

    }

    public static JavaRDD<AlertLog> getAlertLogFromCassandraRows(JavaRDD<Row> cassandraRows)  {

        JavaRDD<AlertLog> alertLogRdd = cassandraRows.map(new Function<Row, AlertLog>() {

            @Override public AlertLog call(Row cassandraRow) throws Exception {
                AlertLog alertLog = new AlertLog();
                alertLog.setResrc_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_RESRC_UID_INDEX));
                alertLog.setDuration(cassandraRow.getInt(QueryUtil.ALERT_LOG_DURATION_INDEX));
                alertLog.setStatus(cassandraRow.getString(QueryUtil.ALERT_LOG_STATUS_INDEX));
                alertLog.setTime_bucket(cassandraRow.getString(QueryUtil.ALERT_LOG_TIME_BUCKET));
                if ( !cassandraRow.isNullAt(QueryUtil.ALERT_LOG_ALERT_TS_INDEX)) {
                    System.out.println("==== Date = " + cassandraRow.get(QueryUtil.ALERT_LOG_ALERT_TS_INDEX).toString());
                    SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
                    String timeString = cassandraRow.get(QueryUtil.ALERT_LOG_ALERT_TS_INDEX).toString();

                    alertLog.setAlert_ts(sdf.parse(timeString));
                }
                alertLog.setResrc_type(cassandraRow.getString(QueryUtil.ALERT_LOG_RESC_TYPE_INDEX));
                alertLog.setSite_name(cassandraRow.getString(QueryUtil.ALERT_LOG_SITE_NAME_INDEX));
                alertLog.setZone_name(cassandraRow.getString(QueryUtil.ALERT_LOG_ZONE_NAME_INDEX));
                alertLog.setAsset_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_RESRC_UID_INDEX));
                alertLog.setSite_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_SITE_UID_INDEX));
                alertLog.setAlert_type(cassandraRow.getString(QueryUtil.ALERT_LOG_ALERT_TYPE_INDEX));
                alertLog.setAlert_name(cassandraRow.getString(QueryUtil.ALERT_LOG_ALERT_NAME_INDEX));
                alertLog.setEnterprise_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_ENTERPRISE_UID_INDEX));
                alertLog.setAlert_ts_tz(cassandraRow.getString(QueryUtil.ALERT_LOG_ALERT_TZ_LOC_INDEX));
                alertLog.setTime_bucket(cassandraRow.getString(QueryUtil.ALERT_LOG_TIME_BUCKET));
                alertLog.setStatus(cassandraRow.getString(QueryUtil.ALERT_LOG_STATUS_INDEX));
                alertLog.setSeverity(cassandraRow.getString(QueryUtil.ALERT_LOG_SEVERITY_INDEX));
                alertLog.setCategory(cassandraRow.getString(QueryUtil.ALERT_LOG_CATEGORY_INDEX));
                alertLog.setAsset_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_ASSET_UID_INDEX));
                alertLog.setAsset_type(cassandraRow.getString(QueryUtil.ALERT_LOG_ASSET_TYPE_INDEX));
                alertLog.setLog_uuid(UUID.fromString(cassandraRow.getString(QueryUtil.ALERT_LOG_LOG_UUID_INDEX)));
                LOG.info("alertLog.cassandraRow.getString: {}", cassandraRow.getString(QueryUtil.ALERT_LOG_LOG_UUID_INDEX));
                LOG.info("alertLog.setLog_uuid: {}", UUID.fromString(cassandraRow.getString(QueryUtil.ALERT_LOG_LOG_UUID_INDEX)));
                if(!cassandraRow.isNullAt(QueryUtil.ALERT_LOG_EXT_PROPS_INDEX)) {
                    HashMap<String, String> extProps = new HashMap<>(cassandraRow.getJavaMap(QueryUtil.ALERT_LOG_EXT_PROPS_INDEX));
                    if(extProps != null) {
                        alertLog.setExt_props(extProps);
                    }
                }
                return alertLog;
            }
        });
        return alertLogRdd;
    }

    /*
     */

    public static JavaRDD<AlertLog> filterAlertLogForRuleNames(List<String> ruleNames, JavaRDD<AlertLog> unfilteredAlerts, String timeZone) {
        return unfilteredAlerts.filter(new Function<AlertLog, Boolean>() {
            @Override
            public Boolean call(AlertLog alertLog) throws Exception {
                LOG.info("filtering alert_log alertLog.getAlert_name(): {}, alertLog.getStatus(): {}", alertLog.getAlert_name(), alertLog.getStatus());
                ruleNames.forEach(e -> LOG.info("rule names in alert_log filtering: {}", e));
                if(ruleNames.contains(alertLog.getAlert_name()) && "active".equalsIgnoreCase(alertLog.getStatus())) {

                    if(timeZone.equalsIgnoreCase(alertLog.getAlert_ts_tz())) {
                        LOG.info("filtering alert_log condition: {} return true" );
                        return true;
                    }
                }
                LOG.info("filtering alert_log condition: {} return false" );
                return false;
            }
        });
    }


    public static JavaRDD<AlertLog> getAlertLogFromAlarmObject(JavaRDD<AlarmObject> alarmsData, String presentTimeBucket, String timeZone) {
        JavaRDD<AlertLog> alertsByMin = alarmsData.map(new Function<AlarmObject, AlertLog>() {

            @Override
            public AlertLog call(AlarmObject alarmObject) throws Exception {
                Map<String, String> extProps = new HashMap<>();
                alarmObject.setSiteId(alarmObject.getExtProps().get("site_uid"));

                alarmObject.getExtProps().entrySet().stream().filter(i -> i.getKey() != null && i.getValue() != null).forEach(x -> extProps.put(x.getKey(), x.getValue()));

                AlertLog alertLog = new AlertLog();
                alertLog.setDuration(alarmObject.getDuration().intValue());
                alertLog.setAlert_name(alarmObject.getAlertName());
                alertLog.setAlert_type(alarmObject.getAlertType());
                alertLog.setCategory(alarmObject.getCategory());
                alertLog.setSeverity(String.valueOf(alarmObject.getSeverity()));

                alertLog.setAlert_ts_tz(timeZone);
                if(alarmObject.getAssetId() != null) {
                    alertLog.setAsset_uid(alarmObject.getAssetId());
                }

                Date utcDate =  Date.from(DateUtils.getZonedDateTimeFromTimeStringTimeZone(alarmObject.getTimeOfAlert(),"yyyyMMddHHmm", timeZone).toInstant());;
                alertLog.setAlert_ts(utcDate);

                Date localDate = Date.from(DateUtils.getZonedDateTimeFromTimeStringTimeZone(alarmObject.getTimeOfAlert(),"yyyyMMddHHmm", "UTC").toInstant());
                alertLog.setAlert_ts_loc(localDate);

                if(alarmObject.getExtProps().get("site_name") != null) {
                    alertLog.setSite_name(alarmObject.getExtProps().get("site_name"));
                } else {
                    alertLog.setSite_name("NONAME");
                }
                if(alarmObject.getExtProps().get("ZT") != null) {
                    alertLog.setZone_name(alarmObject.getExtProps().get("ZT"));
                } else {
                    alertLog.setZone_name("NONAME");
                }
                alertLog.setStatus("active");
                alertLog.setLog_uuid(UUIDs.timeBased());
                alertLog.setEnterprise_uid(alarmObject.getEnterpriseId());
                alertLog.setSite_uid(alarmObject.getSiteId());
                alertLog.setTime_bucket(presentTimeBucket);
                alertLog.setResrc_uid(alarmObject.getResrcUid());
                alertLog.setResrc_type(alarmObject.getResrcType());
                alertLog.setExt_props(extProps);
                return alertLog;
            }

        });
        return alertsByMin;
    }


    public static JavaRDD<AlertLogStg> getAlertLogStgFromAlertLog(JavaRDD<AlertLog> alertData) {
        JavaRDD<AlertLogStg> alertsByMin = alertData.map(new Function<AlertLog, AlertLogStg>() {

            @Override
            public AlertLogStg call(AlertLog alert) throws Exception {
                AlertLogStg alertLogStg = new AlertLogStg();
                Map<String, String> extProps = new HashMap<>();
                if(alert.getExt_props() != null) {
                    alert.getExt_props().entrySet().forEach(x -> extProps.put(x.getKey(), x.getValue()));
                }

                alertLogStg.setDuration(alert.getDuration());
                alertLogStg.setAlert_name(alert.getAlert_name());
                alertLogStg.setAlert_type(alert.getAlert_type());
                alertLogStg.setCategory(alert.getCategory());
                alertLogStg.setSeverity(String.valueOf(alert.getSeverity()));

                alertLogStg.setAlert_ts_tz(alert.getAlert_ts_tz());
                alertLogStg.setAlert_ts(alert.getAlert_ts());
                if(alert.getSite_name() != null) {
                    alertLogStg.setSite_name(alert.getSite_name());
                }
                if(alert.getSite_uid() != null) {
                    alertLogStg.setSite_uid(alert.getSite_uid());
                }
                if(alert.getZone_name() != null) {
                    alertLogStg.setZone_name(alert.getZone_name());
                }
                alertLogStg.setStatus(alert.getStatus());
                alertLogStg.setLog_uuid(alert.getLog_uuid());
                alertLogStg.setEnterprise_uid(alert.getEnterprise_uid());
                alertLogStg.setTime_bucket(alert.getTime_bucket());
                alertLogStg.setResrc_uid(alert.getResrc_uid());
                alertLogStg.setResrc_type(alert.getResrc_type());
                alertLogStg.setExt_props(extProps);
                return alertLogStg;
            }

        });
        return alertsByMin;
    }

    public static JavaPairRDD<String, AlertLog> getAlertLogJavaPairRDD (JavaRDD<AlertLog> alertLogJavaRDD) {
        JavaPairRDD<String, AlertLog> alertLogJavaPairRDD = alertLogJavaRDD.mapToPair(new PairFunction<AlertLog, String, AlertLog>() {
            @Override
            public Tuple2<String, AlertLog> call(AlertLog alertLog) throws Exception {
                return new Tuple2<String, AlertLog>(alertLog.getResrc_uid(), alertLog);
            }
        });
        return alertLogJavaPairRDD;
    }


}
