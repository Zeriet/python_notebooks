package com.ge.current.em.custom.analytics.batch;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.custom.analytics.drools.IERulesUtil;
import com.ge.current.em.entities.analytics.AlarmObject;
import com.ge.current.em.entities.analytics.AlertLog;
import com.ge.current.em.entities.analytics.AlertLogStg;
import com.ge.current.em.entities.analytics.Event;
import com.ge.current.em.entities.analytics.MessageLog;
import com.ge.current.em.entities.analytics.RulesBaseFact;
import com.ge.current.em.util.AlarmsUtil;
import com.ge.current.em.util.DateUtils;
import com.ge.current.em.util.EmUtil;
import com.ge.current.em.util.ErrorLoggingUtil;
import com.ge.current.em.util.QueryUtil;
import com.ge.current.em.util.RulesEmailTemplate;
import com.ge.current.ie.analytics.batch.ISparkBatch;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.h2.result.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public interface ISparkRule extends ISparkBatch {

    Logger LOG = LoggerFactory.getLogger(ISparkRule.class);

    PropertiesBean getPropertiesBean();

    String getMaster();

    String getAppName();

    void setDate(String date);

    String getDate();

    String getRescrType();

    void setRescrType(String rescrType);

    String getGroupBy();

    void setGroupBy(String groupBy);

    String getEndTime();

    void setEndTime(String endTime);

    String getTableName();

    void setTableName(String tableName);

    String getTimeZone();

    void setTimeZone(String timeZone);

    String getRuleNamesAsString();

    String getTimeFrame();

    void setTimeFrame(String timeFrame);

    void setRuleNamesAsString(String ruleNamesAsString);

    void setRuleNames(List<String> rulesNames);

    List<String> getRuleUids();

    void setAppName(String appName);

    /**
     * Get previous timeBucket based on the given jobFrequency.
     * <p>
     *     e.g. currentTimeBucket = 201705031200, jobFrequencyInMinutes = 1440
     *          method will return: 201705021200
     * </p>
     * @param currentTimeBucket
     * @param jobFrequencyInMinutes
     * @return
     */
    String getPreviousTimeBucket(String currentTimeBucket, String jobFrequencyInMinutes) throws ParseException;

    Map<String, String> tagsForMessageLog = new HashMap<>();


    //	public JavaRDD<Row> getRowRDD();

    @Override default void run(Environment environment, String startTime, String endTime, String timeZone) {
        LOG.info("SparkBatchCassandra run method");
        SparkConf sparkConf = (new SparkConf()).setMaster(this.getMaster()).setAppName(this.getAppName());
        if (this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_USERNAME) != null) {
            sparkConf.set("spark.cassandra.auth.username",
                    this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_USERNAME));
        }

        if (this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_PASSWORD) != null) {
            sparkConf.set("spark.cassandra.auth.password",
                    this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_PASSWORD));
        }

        sparkConf.set("spark.cassandra.connection.host",
                this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_CONNECTION_HOST));
        sparkConf.set("spark.cassandra.connection.port",
                this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_CONNECTION_PORT));
        SparkContext sc = new SparkContext(sparkConf);
        CassandraSQLContext cassandraSQLContext = new CassandraSQLContext(sc);
        cassandraSQLContext.setKeyspace(this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_KEYSPACE));
        this.broadcast(sc, this.getPropertiesBean());

        String[] timeZones = timeZone.split(",");
        String ruleNamesAsString = getRuleNamesAsString();
        String[] rules = ruleNamesAsString.split(",");
        String localEndTime =null;
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        for (String tz : timeZones) {
            this.setTimeZone(tz);

            try {
                String localStartTime = DateUtils
                        .convertUTCStringToLocalTimeString(startTime, "yyyyMMddHHmm", tz, "yyyyMMddHHmm");
                 localEndTime = DateUtils
                        .convertUTCStringToLocalTimeString(endTime, "yyyyMMddHHmm", tz, "yyyyMMddHHmm");
                this.setDate(localStartTime);
                this.setEndTime(localEndTime);
                String previousTimeBucket = getPreviousTimeBucket(localEndTime, getTimeFrame());
                DataFrame pastAlertsDataFrame = cassandraSQLContext.sql(QueryUtil.getAlarmsForPastRunQuery(previousTimeBucket,
                        this.getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_KEYSPACE), "alert_log_stg"))
                        .cache();
                DataFrame df = cassandraSQLContext.sql(this.getQuery(this.getPropertiesBean()));
                JavaRDD rowsRDD = df.javaRDD();

                this.processRDD(rowsRDD, pastAlertsDataFrame.javaRDD());


                MessageLog messageLog = ErrorLoggingUtil.createMessageLog(rules, localEndTime, tz, "Rules Job Summary", tagsForMessageLog);
                JavaRDD<MessageLog> messageLogJavaRDD = javaSparkContext.parallelize(Arrays.asList(messageLog));
                ErrorLoggingUtil.writeErrorsToCassandra(this.getPropertiesBean(), messageLogJavaRDD);
            } catch (Exception e) {
                tagsForMessageLog.put("error", e.getMessage());
                MessageLog messageLog = ErrorLoggingUtil.createMessageLog(rules, localEndTime, tz, "Rules_Job", tagsForMessageLog);
                JavaRDD<MessageLog> messageLogJavaRDD = javaSparkContext.parallelize(Arrays.asList(messageLog));
                ErrorLoggingUtil.writeErrorsToCassandra(this.getPropertiesBean(), messageLogJavaRDD);
                StringWriter sw = new StringWriter();
                new Throwable().printStackTrace(new PrintWriter(sw));
                new RulesEmailTemplate(this.getPropertiesBean())
                        .sendEmail(this.getAppName(), sc.applicationId(), e.getMessage(), sw.toString());
                LOG.error(e.getMessage());
            }
        }
    }

    default void processRDD(JavaRDD<Row> rdd, JavaRDD<Row> pastAlertRows) throws ParseException {
        long rowsCountBeforeFilter = rdd.count();
        LOG.warn("rdd after query: " + rowsCountBeforeFilter);
        tagsForMessageLog.put(ErrorLoggingUtil.BEFORE_FILTER_COUNT, String.valueOf(rowsCountBeforeFilter));
        List<String> rulesNames = new ArrayList<>();
        String ruleNamesAsString = getRuleNamesAsString();

        String[] rules = ruleNamesAsString.split(",");
        for (String rule : rules) {
            rulesNames.add(rule);
        }
        setRuleNames(rulesNames);

        JavaRDD<Row> filteredRows = getFilteredInput(rdd).cache();
        long rowsCountAfterFilter = filteredRows.count();

        tagsForMessageLog.put(ErrorLoggingUtil.AFTER_FILTER_COUNT, String.valueOf(rowsCountAfterFilter));
        JavaRDD<RulesBaseFact> rulesFact = generateRuleData(filteredRows).cache();
        JavaRDD<AlarmObject> alarmsObjects = triggerRule(rulesFact).cache();

        saveAlarmsData(alarmsObjects, pastAlertRows);
    }

    JavaRDD<Row> getFilteredInput(JavaRDD<Row> df);

    JavaRDD<RulesBaseFact> generateRuleData(JavaRDD<Row> rowsRDD);

    default JavaRDD<AlarmObject> triggerRule(JavaRDD<RulesBaseFact> rulesFact) {
        LOG.info("triggering rule: trying to trigger the rule");
        List<String> rulesList = getRulesStringList();
        JavaRDD<AlarmObject> alarms = rulesFact
                .mapPartitions(new FlatMapFunction<Iterator<RulesBaseFact>, AlarmObject>() {

                    List<AlarmObject> alarms = new ArrayList<>();

                    public Iterable<AlarmObject> call(Iterator<RulesBaseFact> rulesBaseFact) throws ParseException {
                        IERulesUtil.initRulesEngine(rulesList);
                        while (rulesBaseFact.hasNext()) {
                            RulesBaseFact ruleFact = rulesBaseFact.next();
                            try {
                                IERulesUtil.applyRule(ruleFact);
                            } catch (Exception e) {
                                LOG.error("Exception in applying rule for: {} \n\t\tReason: {}", ruleFact.getAssetId(),
                                        e.getMessage());
                            }
                            LOG.info("ConditionMet: " + ruleFact.getConditionMet());
                            if (ruleFact.getConditionMet()) {
                                AlarmObject alarm = ruleFact.getAlarmObject();
                                LOG.info(" Alarm data for assetId: {}, alarmData {}", alarm.getResrcUid(), alarm);
                                alarm.setAlertName(alarm.getAlertName());
                                alarm.setAssetType(ruleFact.getAssetType());
                                if (ruleFact.getExt_properties().get("assetName") != null) {
                                    alarm.setAssetName(ruleFact.getExt_properties().get("assetName").toString());
                                    LOG.info("ruleFact.getExt_properties().get(assetName) {}",
                                            ruleFact.getExt_properties().get("assetName"));

                                }
                                alarm.setEnterpriseId(ruleFact.getEnterpriseId());
                                if (ruleFact.getExt_properties().get("site_uid") != null) {
                                    alarm.setSiteId(ruleFact.getExt_properties().get("site_uid").toString());
                                    LOG.info("ruleFact.getExt_properties().get(site_uid) {} ",
                                            ruleFact.getExt_properties().get("site_uid"));

                                }
                                alarm.setTimeOfAlert(getStartDate(getDate()) + ruleFact.getHhmm());
                                alarm.setSiteId(ruleFact.getExt_properties().get("site_uid"));
                                if ("site".equalsIgnoreCase(getGroupBy())) {
                                    alarm.setResrcUid(alarm.getSiteId() + "." + alarm.getAlertName() + "." + alarm.getAlertName());
                                    alarm.setAssetId("NONAME");
                                    alarm.setAssetName("NONAME");
                                    alarm.setResrcType("SITE");
                                } else {
                                    alarm.setResrcUid(alarm.getAssetId() + "." + alarm.getAlertName());
                                    alarm.setResrcType("ASSET");
                                }
                                Map<String, String> extProps = new HashMap<>();
                                ruleFact.getExt_properties().entrySet().stream()
                                        .filter(i -> i.getKey() != null && i.getValue() != null)
                                        .forEach(x -> extProps.put("ext_props" + x.getKey(), x.getValue()));

                                extProps.put("ext_propsfaultCategory", alarm.getFaultCategory());
                                if (alarm.getFrequency() != null) {
                                    extProps.put("ext_propsfrequency", alarm.getFrequency());
                                }
                                extProps.put("ext_propstablename", alarm.getQueryTable());
                                extProps.put("ext_propsprofileName", alarm.getAlertName());
                                // todo set am.setExtProps before this line
                                if (extProps != null && extProps.get("ext_propssite_uid") != null
                                        && extProps.get("ext_propsLT") != null) {
                                    extProps.put("ext_propssiteLtRuleGroup",
                                            extProps.get("ext_propssite_uid") + "." + extProps.get("ext_propsLT") + "."
                                                    + alarm.getAlertName() + "." + alarm.getAlertName());
                                }

                                alarm.setExtProps(extProps);
                                alarms.add(alarm);
                            }

                        }
                        return alarms;
                    }
                });
        return alarms;

    }

    public String getStartDate(String time) throws ParseException;

    public String getEndDate(String time) throws ParseException;

    default void saveAlarmsData(JavaRDD<AlarmObject> alarmsData, JavaRDD<Row> pastAlertRows) throws ParseException {
        String currentTimeBucket = getEndTime();
        String previousTimeBucket = getPreviousTimeBucket(currentTimeBucket, getTimeFrame());

        JavaRDD<AlertLog> currentAlertRdd = AlarmsUtil.getAlertLogFromAlarmObject(alarmsData, currentTimeBucket, getTimeZone());
        JavaRDD<AlertLog> pastAlertRdd = AlarmsUtil.getAlertLogFromCassandraRows(pastAlertRows);

        long totalPresentAlerts = currentAlertRdd.count();
        long totalPastAlerts = pastAlertRdd.count();
        tagsForMessageLog.put(ErrorLoggingUtil.TOTAL_PAST_ALERTS, String.valueOf(totalPastAlerts));
        tagsForMessageLog.put(ErrorLoggingUtil.TOTAL_PRESENT_ALERTS, String.valueOf(totalPresentAlerts));
        JavaRDD<AlertLog> finalAlertRdd = AlarmsUtil
                .getAllTheAlarmsToBeSaved(currentAlertRdd, pastAlertRdd, currentTimeBucket, previousTimeBucket, getRuleUids(),
                        getTimeZone());
        long finalAlertRddCount = finalAlertRdd.count();
        LOG.info("saving the alert final alert: {}", finalAlertRddCount);
        javaFunctions(finalAlertRdd)
                .writerBuilder(getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_KEYSPACE), "alert_log",
                        mapToRow(AlertLog.class)).saveToCassandra();
        JavaRDD<AlertLogStg> finalAlertLogStgRdd = AlarmsUtil.getAlertLogStgFromAlertLog(finalAlertRdd).cache();
        javaFunctions(finalAlertLogStgRdd)
                .writerBuilder(getPropertiesBean().getValue(SparkConfigurations.CASSANDRA_KEYSPACE), "alert_log_stg",
                        mapToRow(AlertLogStg.class)).saveToCassandra();

        tagsForMessageLog.put(ErrorLoggingUtil.TOTAL_SAVED_ALERTS, String.valueOf(finalAlertRddCount));
    }

    Map<String, Object> getParametersMap(String enterpriseId, String siteId);

	
	/*
     * Check to see if the tags contains all the attributes needed for the rule
	 *
	 * @return
	 * boolean
	 */

    default boolean containsRequiredMeasuresAvg(Map<String, Double> measures) {
        List<String> requiredMeasuresAvgList = getListOfRequiredMeasuresAvg(null);
        if (requiredMeasuresAvgList == null) {
            return true;
        }
        for (String key : requiredMeasuresAvgList) {
            String[] tag = key.split(",");
            boolean contains = false;
            for (String tagArray : tag) {
                LOG.info("containsRequiredTags Key: {}, contains: {} ", key, measures.get("measures_avg"+tagArray));
                if (measures.containsKey("measures_avg" + tagArray)) {
                    contains = true;
                }
            }
            if (!contains) {
                return false;
            }
        }
        return true;
    }

    /*
     * Check to see if the tags contains all the attributes needed for the rule
     *
     * @return
     * boolean
     */
    default boolean containsRequiredMeasuresAggr(Map<String, Double> measures) {
        List<String> requiredMeasuresAggrList = getListOfRequiredMeasuresAggr(null);
        if (requiredMeasuresAggrList == null) {
            return true;
        }
        for (String key : requiredMeasuresAggrList) {
            String[] tag = key.split(",");
            boolean contains = false;
            for (String tagArray : tag) {
                LOG.info("containsRequiredTags Key: {}, contains: {} ", key, measures.get("measures_aggr" + tagArray));
                if (measures.containsKey("measures_aggr" + tagArray)) {
                    contains = true;
                }
            }
            if (!contains) {
                return false;
            }
        }
        return true;
    }

    /*
     * Check to see if the measures contains all the attributes needed for the rule
     *
     * @return
     * boolean
     */
    default boolean containsRequiredTags(Map<String, String> tags) {
        List<String> requiredTagsList = getListOfRequiredTags(null);
        if (requiredTagsList == null) {
            return true;
        }
        for (String key : requiredTagsList) {
            String[] tag = key.split(",");
            boolean contains = false;
            for (String tagArray : tag) {
                LOG.info("containsRequiredTags Key: {}, contains: {} ", key, tags.get("tags"+tagArray));
                if (tags.containsKey("tags" + tagArray)) {
                    contains = true;
                }
            }
            if (!contains) {
                return false;
            }
        }
        return true;
    }

    List<String> getListOfRequiredTags(List<String> ruleIds);

    /*
     * Check to see if the assetType matches the one for the rule coming from the events.
     *
     * @return
     * boolean
     */
    default boolean containsRequiredAssetTypes(String assetType) {
        List<String> registeredAssetTypes = getListOfRegisteredAssetsType(null);
        LOG.info("required asset types: {}", registeredAssetTypes);

        if (registeredAssetTypes != null) {
            for (String asset : registeredAssetTypes) {
                if (asset.equalsIgnoreCase(assetType)) {
                    return true;
                }
            }
        }
        return false;
    }

    List<String> getListOfRegisteredAssetsType(List<String> ruleIds);

    List<String> getRulesStringList();

    String getTimeBucket(String startTime, String endDateTime) throws ParseException;

    List<String> getListOfRequiredMeasures(List<String> ruleIds);

    List<String> getListOfRequiredMeasuresAvg(List<String> ruleIds);

    List<String> getListOfRequiredMeasuresAggr(List<String> ruleIds);

    /**
     * Generate the CQL query to fetch the required rows
     * from Cassandra
     *
     * @return The CQL query to get the required rows.
     */
    String getQuery(PropertiesBean propertiesBean);

    String getActiveProfile();

    default void initializeRulesBaseFactFromEvents(Event event, RulesBaseFact rulesBaseFact) {
        rulesBaseFact.setAssetId(event.getAssetId());
        rulesBaseFact.setConditionMet(false);
        rulesBaseFact.setParameters(getParametersMap(event.getEnterpriseId(), event.getSiteId()));
        LOG.info("Initializing rulesBaseFact: siteId {}, enterpriseId : {} ", event.getSiteId(),
                event.getEnterpriseId());
        rulesBaseFact.setAlarmObject(new AlarmObject());
        rulesBaseFact.setTagsMap(new ArrayList<>());
        rulesBaseFact.setEnterpriseId(event.getEnterpriseId());
        rulesBaseFact.setSegmentId(event.getSegmentId());
        rulesBaseFact.setSiteId(event.getSiteId());
        rulesBaseFact.setHhmm(event.getHhmm());
        rulesBaseFact.setAssetType(event.getAssetType());
    }
}
