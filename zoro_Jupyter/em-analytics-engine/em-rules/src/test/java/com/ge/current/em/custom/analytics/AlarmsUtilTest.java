package com.ge.current.em.custom.analytics;//package com.ge.current.em.custom.analytics;

import com.ge.current.em.entities.analytics.AlertLog;
import com.ge.current.em.util.AlarmsUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by 212565238 on 2/25/17.
 */

public class AlarmsUtilTest implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AlarmsUtilTest.class);

    private transient SparkConf sparkConf;
    private transient SparkContext sc;
    private transient JavaSparkContext jsc;
    private AlarmsUtil alarmsUtil;


    @Before
    public void setUp() {

        sparkConf = (new SparkConf()).setAppName("testApp").setMaster("local[*]");
        sc = new SparkContext(sparkConf);
        jsc = new JavaSparkContext(sc);
        alarmsUtil = new AlarmsUtil();

    }

    @After
    public void tearDown() {
        jsc.stop();
        jsc = null;
    }


    @Test
    public void itShouldMatchOutputCount() {

        JavaRDD<Row> inputAlertRows = rowsFromAlertCreator(getInputAlertLog());
        JavaRDD<Row> pastAlertRows = rowsFromAlertCreator(getPastAlertLog());
        List<String> ruleNames = new ArrayList<>();
        ruleNames.add("Too cold threshold");
        ruleNames.add("Too hot threshold");
        ruleNames.add("Excessive HVAC");
        JavaRDD<AlertLog> inputAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(inputAlertRows);
        inputAlertLog.collect().forEach(e -> LOG.info("********* Input alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> pastAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(pastAlertRows);
        pastAlertLog.collect().forEach(e -> LOG.info("********* pastAlertRows resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> agggregatedAlerts = AlarmsUtil.getAllTheAlarmsToBeSaved(inputAlertLog,pastAlertLog, "201702281100", "201702281000",ruleNames, "PST");
        agggregatedAlerts.collect().forEach(e -> LOG.info("********* aggregated alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));

        JavaRDD<AlertLog> finalAlertRdd = jsc.parallelize(getFinalAlertLog());
        finalAlertRdd.collect().forEach(e -> LOG.info("********* final alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        assertEquals(finalAlertRdd.count(),agggregatedAlerts.count());

    }

    @Test
    public void itShouldMatchPastCount() {

        JavaRDD<Row> inputAlertRows = rowsFromAlertCreator(getInputAlertLog());
        JavaRDD<Row> pastAlertRows = rowsFromAlertCreator(getPastAlertLog());
        List<String> ruleNames = new ArrayList<>();
        ruleNames.add("Too cold threshold");
        ruleNames.add("Too hot threshold");
        ruleNames.add("Excessive HVAC");
        JavaRDD<AlertLog> inputAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(inputAlertRows);
        inputAlertLog.collect().forEach(e -> LOG.info("********* Input alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> pastAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(pastAlertRows);
        pastAlertLog.collect().forEach(e -> LOG.info("********* pastAlertRows resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> agggregatedAlerts = AlarmsUtil.getAllTheAlarmsToBeSaved(inputAlertLog,pastAlertLog, "201702281100", "201702281000",ruleNames, "PST");


        agggregatedAlerts.collect().forEach(e -> LOG.info("********* aggregated alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));

        JavaRDD<AlertLog> pastAlerts = agggregatedAlerts.filter(alertLog -> "past".equalsIgnoreCase(alertLog.getStatus()));
        JavaRDD<AlertLog> finalAlertRdd = jsc.parallelize(getFinalAlertLog());
        finalAlertRdd.collect().forEach(e -> LOG.info("********* final alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        assertEquals(pastAlerts.count(),2);

    }

    @Test
    public void itShouldMatchActiveCount() {

        JavaRDD<Row> inputAlertRows = rowsFromAlertCreator(getInputAlertLog());
        JavaRDD<Row> pastAlertRows = rowsFromAlertCreator(getPastAlertLog());
        List<String> ruleNames = new ArrayList<>();
        ruleNames.add("Too cold threshold");
        ruleNames.add("Too hot threshold");
        ruleNames.add("Excessive HVAC");
        JavaRDD<AlertLog> inputAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(inputAlertRows);
        inputAlertLog.collect().forEach(e -> LOG.info("********* Input alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> pastAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(pastAlertRows);
        pastAlertLog.collect().forEach(e -> LOG.info("********* pastAlertRows resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> agggregatedAlerts = AlarmsUtil.getAllTheAlarmsToBeSaved(inputAlertLog,pastAlertLog, "201702281100", "201702281000",ruleNames, "PST").filter(alertLog -> "active".equalsIgnoreCase(alertLog.getStatus()));
        agggregatedAlerts.collect().forEach(e -> LOG.info("********* aggregated alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));

        assertEquals(agggregatedAlerts.count(),2);

    }

    @Test
    public void itShouldMatchInActiveCount() {

        JavaRDD<Row> inputAlertRows = rowsFromAlertCreator(getInputAlertLog());
        JavaRDD<Row> pastAlertRows = rowsFromAlertCreator(getPastAlertLog());
        List<String> ruleNames = new ArrayList<>();
        ruleNames.add("Too cold threshold");
        ruleNames.add("Too hot threshold");
        ruleNames.add("Excessive HVAC");
        JavaRDD<AlertLog> inputAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(inputAlertRows);
        inputAlertLog.collect().forEach(e -> LOG.info("********* Input alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> pastAlertLog = AlarmsUtil.getAlertLogFromCassandraRows(pastAlertRows);
        pastAlertLog.collect().forEach(e -> LOG.info("********* pastAlertRows resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> agggregatedAlerts = AlarmsUtil.getAllTheAlarmsToBeSaved(inputAlertLog,pastAlertLog, "201702281100", "201702281000",ruleNames, "PST");
        agggregatedAlerts.collect().forEach(e -> LOG.info("********* aggregated alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        JavaRDD<AlertLog> inactiveAlerts = agggregatedAlerts.filter(alertLog -> "inactive".equalsIgnoreCase(alertLog.getStatus()));
        inactiveAlerts.collect().forEach(e -> LOG.info("********* final alerts resrouce_uid: {} , durations: {}, status: {}",e.getResrc_uid(), e.getDuration(),e.getStatus()));
        assertEquals(inactiveAlerts.count(),1);

    }

    public JavaRDD<Row> rowsFromAlertCreator(List<AlertLog> alertLogs) {
        JavaRDD<Row> unfilterdRows = null;

        unfilterdRows = jsc.parallelize(alertLogs).map(alertLog -> RowFactory
                .create(alertLog.getTime_bucket(),alertLog.getResrc_uid(),alertLog.getResrc_type(),alertLog.getEnterprise_uid(),
                        alertLog.getAlert_ts(), alertLog.getAlert_ts_tz(), alertLog.getLog_uuid().toString(),
                        alertLog.getAlert_type(), alertLog.getAlert_name(),alertLog.getSeverity(), alertLog.getCategory(),
                        alertLog.getDuration(),alertLog.getStatus(),alertLog.getSite_uid(), alertLog.getSite_name(),alertLog.getZone_name(),
                        alertLog.getAsset_uid(),alertLog.getAsset_type(),alertLog.getExt_props()));

        return unfilterdRows;
    }

    public <K, V> scala.collection.immutable.Map<K, V> convert(Map<K, V> m) {
        return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala()
                .toMap(scala.Predef$.MODULE$.<scala.Tuple2<K, V>>conforms());
    }


    private List<AlertLog> getInputAlertLog() {
        List<AlertLog> alertLogs = new ArrayList<>();
        AlertLog alertLog = new AlertLog();
        alertLog.setResrc_uid("abcd.Too hot threshold");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(15);
        alertLog.setTime_bucket("201702281100");
        alertLog.setAlert_ts_tz("201702281100");
        alertLog.setStatus("active");
        alertLog.setAlert_name("Too hot threshold");
        alertLog.setAlert_type("Too hot threshold");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);
        alertLog =  new AlertLog();
        alertLog.setResrc_uid("xyz.Too cold threshold");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(30);
        alertLog.setTime_bucket("201702281100");
        alertLog.setAlert_ts_tz("201702281100");
        alertLog.setStatus("active");
        alertLog.setAlert_name("Too cold threshold");
        alertLog.setAlert_type("Too cold threshold");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);
        return alertLogs;
    }

    private List<AlertLog> getInputForPastAlertLog() {
        List<AlertLog> alertLogs = new ArrayList<>();

        return alertLogs;
    }

    private List<AlertLog> getPastAlertLog() {
        List<AlertLog> alertLogs = new ArrayList<>();
        AlertLog alertLog = new AlertLog();
        alertLog.setResrc_uid("abcd.Too hot threshold");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(5);
        alertLog.setTime_bucket("201702281000");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("active");
        alertLog.setAlert_name("Too hot threshold");
        alertLog.setAlert_type("Too hot threshold");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);
        alertLog =  new AlertLog();
        alertLog.setResrc_uid("lmn.Excessive HVAC");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(10);

        alertLog.setTime_bucket("201702281000");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("Inactive");
        alertLog.setAlert_name("Excessive HVAC");
        alertLog.setAlert_type("Excessive HVAC");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);

        alertLog =  new AlertLog();
        alertLog.setResrc_uid("opq.Excessive HVAC");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(10);

        alertLog.setTime_bucket("201702281000");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("active");
        alertLog.setAlert_name("Excessive HVAC");
        alertLog.setAlert_type("Excessive HVAC");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);
        return  alertLogs;
    }

    private List<AlertLog> getFinalAlertLog() {
        List<AlertLog> alertLogs = new ArrayList<>();
        AlertLog alertLog = new AlertLog();
        alertLog.setResrc_uid("abcd.Too hot threshold");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(20);
        alertLog.setTime_bucket("201702281100");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("active");
        alertLog.setAlert_name("Too hot threshold");
        alertLog.setAlert_type("Too hot threshold");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);
        alertLog =  new AlertLog();
        alertLog.setResrc_uid("xyz.Too cold threshold");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(30);
        alertLog.setTime_bucket("201702281100");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("active");
        alertLog.setAlert_name("Too cold threshold");
        alertLog.setAlert_type("Too cold threshold");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);
        alertLog = new AlertLog();
        alertLog.setResrc_uid("abcd.Too hot threshold");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(5);
        alertLog.setTime_bucket("201702281000");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("past");
        alertLog.setAlert_name("Too hot threshold");
        alertLog.setAlert_type("Too hot threshold");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);

        alertLog =  new AlertLog();
        alertLog.setResrc_uid("opq.Excessive HVAC");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(10);

        alertLog.setTime_bucket("201702281000");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("Inactive");
        alertLog.setAlert_name("Excessive HVAC");
        alertLog.setAlert_type("Excessive HVAC");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);

        alertLog =  new AlertLog();
        alertLog.setResrc_uid("opq.Excessive HVAC");
        alertLog.setResrc_type("asset");
        alertLog.setDuration(10);

        alertLog.setTime_bucket("201702281100");
        alertLog.setAlert_ts_tz("PST");
        alertLog.setStatus("past");
        alertLog.setAlert_name("Excessive HVAC");
        alertLog.setAlert_type("Excessive HVAC");
        alertLog.setLog_uuid(UUID.fromString("6aed7250-e2ab-11e6-b0f8-5f76e7d19639"));
        alertLogs.add(alertLog);
        return alertLogs;
    }
}
