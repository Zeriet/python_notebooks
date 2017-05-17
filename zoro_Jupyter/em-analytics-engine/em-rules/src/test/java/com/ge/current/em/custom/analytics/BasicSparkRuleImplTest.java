package com.ge.current.em.custom.analytics;

import com.ge.current.em.custom.analytics.batch.BasicISparkRuleImpl;
import com.ge.current.em.entities.analytics.Event;
import com.ge.current.em.entities.analytics.RulesBaseFact;
import com.holdenkarau.spark.testing.JavaRDDComparisons;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import scala.collection.JavaConverters$;

import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by 212577826 on 2/8/17.
 */
public class BasicSparkRuleImplTest implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(BasicISparkRuleImpl.class);

    private transient SparkConf sparkConf;
    private transient SparkContext sc;
    private transient JavaSparkContext jsc;
    private BasicISparkRuleImpl basicISparkRule;
    private String timeFrame = "60";
    private String timeZone = "PST";
    private String date = "201702081300";
    private String endDateTime = "201702081400";
    private String dateEvent = "20170208";
    private String hhmmEvent = "1315";
    private Broadcast<Map<String, List<String>>> ruleRequiredAssetTypeMapBroadcast = null;
    private Broadcast<Map<String, List<String>>> ruleRequiredMeasuresAggrMapBroadcast = null;
    private Broadcast<Map<String, List<String>>> ruleRequiredMeasuresAvgMapBroadcast = null;
    private Broadcast<Map<String, List<String>>> ruleRequiredTagsMapBroadcast = null;
    private Broadcast<Map<String, Map<String, Object>>> parameterMapBroadcast = null;
    private Broadcast<Map<String, String>> ruleRegistryMapBroadcast = null;
    private Broadcast<Map<String, String>> ruleNamesMappingBroadcast = null;

    @Before public void setUp() {

        sparkConf = (new SparkConf()).setAppName("testApp").setMaster("local[*]");
        sc = new SparkContext(sparkConf);
        jsc = new JavaSparkContext(sc);
        basicISparkRule = new BasicISparkRuleImpl();
        basicISparkRule.setGroupBy("asset");
        basicISparkRule.setTimeFrame(this.timeFrame);
        basicISparkRule.setTimeZone(this.timeZone);
        basicISparkRule.setDate(this.date);
        basicISparkRule.setTableName("events_byminute_stg");
        basicISparkRule.setEndTime(this.endDateTime);
        ruleRequiredAssetTypeMapBroadcast = sc.broadcast(TestMapUtil.getRuleRequiredAssetType(),
                scala.reflect.ClassTag$.MODULE$.apply(TestMapUtil.getRuleRequiredAssetType().getClass()));
        ruleRequiredMeasuresAggrMapBroadcast = sc.broadcast(TestMapUtil.getRuleRequiredMeasuresMapToBroadCast(),
                scala.reflect.ClassTag$.MODULE$.apply(TestMapUtil.getRuleRequiredMeasuresMapToBroadCast().getClass()));
        ruleRequiredMeasuresAvgMapBroadcast = sc.broadcast(TestMapUtil.getRuleRequiredMeasuresMapToBroadCast(),
                scala.reflect.ClassTag$.MODULE$.apply(TestMapUtil.getRuleRequiredMeasuresMapToBroadCast().getClass()));
        ruleRequiredTagsMapBroadcast = sc.broadcast(TestMapUtil.getRuleRequiredTagsMapToBroadCast(),
                scala.reflect.ClassTag$.MODULE$.apply(TestMapUtil.getRuleRequiredTagsMapToBroadCast().getClass()));
        parameterMapBroadcast = sc.broadcast(TestMapUtil.getParametrsMapToBroadcast(),
                scala.reflect.ClassTag$.MODULE$.apply(TestMapUtil.getParametrsMapToBroadcast().getClass()));
        ruleNamesMappingBroadcast = sc.broadcast(TestMapUtil.getRuleNames(),
                scala.reflect.ClassTag$.MODULE$.apply(TestMapUtil.getRuleNames().getClass()));
        ruleRegistryMapBroadcast = sc.broadcast(TestMapUtil.getRuleRegistryNames(),
                scala.reflect.ClassTag$.MODULE$.apply(TestMapUtil.getRuleRegistryNames().getClass()));


        /* setting up the field that are retrieved from database */
        ReflectionTestUtils.setField(basicISparkRule, "ruleNames", Arrays.asList("RULE-PROP-UID-02"));
        ReflectionTestUtils
                .setField(basicISparkRule, "ruleRequiredAssetTypeMapBroadcast", ruleRequiredAssetTypeMapBroadcast);
        ReflectionTestUtils.setField(basicISparkRule, "ruleRequiredMeasuresAggrMapBroadcast",
                ruleRequiredMeasuresAggrMapBroadcast);
        ReflectionTestUtils
                .setField(basicISparkRule, "ruleRequiredMeasuresAvgMapBroadcast", ruleRequiredMeasuresAvgMapBroadcast);
        ReflectionTestUtils.setField(basicISparkRule, "ruleRequiredTagsMapBroadcast", ruleRequiredTagsMapBroadcast);
        ReflectionTestUtils.setField(basicISparkRule, "parameterMapBroadcast", parameterMapBroadcast);
        ReflectionTestUtils.setField(basicISparkRule, "ruleNamesMappingBroadcast", ruleNamesMappingBroadcast);
        ReflectionTestUtils.setField(basicISparkRule, "ruleRegistryMapBroadcast", ruleRegistryMapBroadcast);


    }

    @After public void tearDown() {
        jsc.stop();
    }

    /* An unflitered event is passes and will return all withour filtering, since all the events pass the filter criteria*/
    @Test public void itShouldReturnAllUnFilteredInput() {

        JavaRDD<Row> unfilteredRows = rowsFromEventsCreator(eventUnfiltered());
        unfilteredRows.collect().forEach(System.out::println);
        LOG.info(" =======filteredRows count before filter {}", unfilteredRows.count());
        JavaRDD<Row> actualRows = basicISparkRule.getFilteredInput(unfilteredRows);
        LOG.info(" =======filteredRows count after filter {}", actualRows.count());
        actualRows.collect().forEach(System.out::println);
        JavaRDDComparisons.assertRDDEquals(actualRows, unfilteredRows);
        JavaRDDComparisons.assertRDDEqualsWithOrder(actualRows, unfilteredRows);
    }

    @Test public void itShouldReturnEmptyRows() {

        JavaRDD<Row> unfilteredRows = rowsFromEventsCreator(eventWithoutTagsRequired());
        unfilteredRows.collect().forEach(System.out::println);
        long unfilteredRowsCount = unfilteredRows.count();
        LOG.info(" =======filteredRows count before filter {}", unfilteredRowsCount);
        JavaRDD<Row> actualRows = basicISparkRule.getFilteredInput(unfilteredRows);
        long filteredRowsCount = actualRows.count();
        LOG.info(" =======filteredRows count after filter {}", actualRows.count());
        actualRows.collect().forEach(System.out::println);
        long expectedFilteredCounts = 0; //since all of the provided events have incorrect Measures Tags, they will fail to pass
        assertEquals(expectedFilteredCounts, filteredRowsCount);

    }

    @Test public void itShouldReturnCorrectFilteredMissingMeasuresTags() {

        JavaRDD<Row> unfilteredRows = rowsFromEventsCreator(eventUnfilteredwithSomeIncorrectAssetTag());
        unfilteredRows.collect().forEach(System.out::println);
        long unfilteredRowsCount = unfilteredRows.count();
        LOG.info(" =======filteredRows count before filter {}", unfilteredRowsCount);
        JavaRDD<Row> actualRows = basicISparkRule.getFilteredInput(unfilteredRows);
        long filteredRowsCount = actualRows.count();
        LOG.info(" =======filteredRows count after filter {}", actualRows.count());
        actualRows.collect().forEach(System.out::println);
        long expectedFilteredCounts = 4; //since 6 of the provided events have resrType as NONASSET, they will fail to pass
        assertEquals(expectedFilteredCounts, filteredRowsCount);
    }

    // TODO: fix me
    @Test public void itShouldReturnCorrectRuleBaseFacts() {

        JavaRDD<Row> inputRows = rowsFromEventsCreator(eventUnfiltered());
        inputRows.collect().forEach(System.out::println);
        long unfilteredRowsCount = inputRows.count();
        LOG.info(" =======filteredRows count before filter {}", unfilteredRowsCount);
        JavaRDD<Row> filteredData = basicISparkRule.getFilteredInput(inputRows);
        LOG.info(" =======filteredRows count after filter {}", filteredData.count());
        JavaRDD<RulesBaseFact> actualRuleBaseFacts = basicISparkRule.generateRuleData(filteredData);
        long generatedRuleBaseFactsCount = 0;
        if(actualRuleBaseFacts != null || !actualRuleBaseFacts.isEmpty()) {
            generatedRuleBaseFactsCount= actualRuleBaseFacts.count();
        }

        LOG.info(" ======= RuleFacts count after generaterule {}", generatedRuleBaseFactsCount);
        actualRuleBaseFacts.collect().forEach(System.out::println);
        long expectedFilteredCounts = 2; //since there are two types of assetId, two ruleBaseFacts should be generated
        assertEquals(expectedFilteredCounts, generatedRuleBaseFactsCount);
    }

    @Test public void itShouldReturnEmptyRuleBaseFacts() {

//        JavaRDD<Row> inputRows = rowsFromEventsCreator((eventUnfilteredwithSomeIncorrectAssetTag()));
//        inputRows.collect().forEach(System.out::println);
//        long unfilteredRowsCount = inputRows.count();
//        JavaRDD<Row> filteredData = basicISparkRule.getFilteredInput(inputRows);
//        LOG.info(" =======filteredRows count before filter {}", unfilteredRowsCount);
//        JavaRDD<RulesBaseFact> actualRuleBaseFacts = basicISparkRule.generateRuleData(filteredData);
//        long generatedRuleBaseFactsCunt= actualRuleBaseFacts.count();
//        LOG.info(" ======= RuleFacts count after generaterule {}", generatedRuleBaseFactsCunt);
//        actualRuleBaseFacts.collect().forEach(System.out::println);
//        JavaRDDComparisons.assertTrue(actualRuleBaseFacts.isEmpty()); //since assetID is 0, no ruleBase facts should be created
    }

    @Test
    public void itShouldReturnPreviousTimeBucketBasedOnJobFrequency() throws ParseException {
        String previousTimeBucket = basicISparkRule.getPreviousTimeBucket("201705031200", "1440");
        assertEquals("201705021200", previousTimeBucket);
    }

    @Test
    public void itShouldReturnPreviousTimeBucketIfJobFrequencyIsNegative() throws ParseException {
        String previousTimeBucket = basicISparkRule.getPreviousTimeBucket("201705031200", "-1440");
        assertEquals("201705021200", previousTimeBucket);
    }

    public JavaRDD<Row> rowsFromEventsCreator(List<Event> events) {
        JavaRDD<Row> unfilterdRows = null;

        unfilterdRows = jsc.parallelize(events).map(event -> RowFactory
                .create(event.getAssetId(), event.getAssetType(), convert(event.getExtProps()),
                        convert(event.getMeasuresAvg()), convert(event.getMeasuresAggr()), convert(event.getTags()),
                        event.getYymmdd(), event.getHhmm(), event.getEnterpriseId()));

        return unfilterdRows;
    }

    public <K, V> scala.collection.immutable.Map<K, V> convert(Map<K, V> m) {
        return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala()
                .toMap(scala.Predef$.MODULE$.<scala.Tuple2<K, V>>conforms());
    }

    public List<Event> eventUnfiltered() {

        List<Event> eventList = new ArrayList<>();
        Event event = new Event();

        Map<String, String> ext_props = new HashMap<>();
        ext_props.put("asset_type", "rtu");
        ext_props.put("asset_type", "rtu");

        Map<String, Object> measuresAgg = new HashMap<>();
        measuresAgg.put("measures_aggrzoneAirTempSensor", "280");
        measuresAgg.put("measures_aggrzoneAirTempOccCoolingSp", "268");

        Map<String, Object> measuresAvg = new HashMap<>();
        measuresAvg.put("measures_avgzoneAirTempSensor", "70");
        measuresAvg.put("measures_avgzoneAirTempOccCoolingSp", "67");

        Map<String, String> tags = new HashMap<>();
        tags.put("runttime_on_stage1", "10");
        tags.put("runttime_on_stage2", "20");

        for (int i = 0; i < 20; i++) {
            String assetId = "assetId001";
            if (i >= 10) assetId = "assetId002";
            event.setAssetId(assetId);
            event.setHhmm(this.hhmmEvent);
            event.setHh("1300");
            event.setAssetType("ASSET");
            event.setTags(tags);
            event.setEnterpriseId("enterprise_GE");
            event.setMeasuresAggr(measuresAgg);
            event.setMeasuresAvg(measuresAvg);
            event.setExtProps(ext_props);
            event.setSiteId("siteId1");
            event.setYymmdd(dateEvent);
            event.setHhmm(hhmmEvent);
            eventList.add(event);
            event = new Event();
        }

        return eventList;
    }

    public List<Event> eventUnfilteredwithSomeIncorrectAssetTag() {

        List<Event> eventList = new ArrayList<>();
        Event event = new Event();

        Map<String, String> ext_props = new HashMap<>();
        ext_props.put("asset_type", "rtu");
        ext_props.put("asset_type", "rtu");

        Map<String, Object> measuresAgg = new HashMap<>();
        measuresAgg.put("measures_aggrzoneAirTempSensor", "280");
        measuresAgg.put("measures_aggrzoneAirTempOccCoolingSp", "268");

        Map<String, Object> measuresAvg = new HashMap<>();
        measuresAvg.put("measures_avgzoneAirTempSensor", "70");
        measuresAvg.put("measures_avgzoneAirTempOccCoolingSp", "67");

        Map<String, String> tags = new HashMap<>();
        tags.put("runttime_on_stage1", "10");
        tags.put("runttime_on_stage2", "20");


        for (int i = 0; i < 10; i++) {
            event = new Event();
            event.setAssetId("lmn");
            event.setHhmm(this.hhmmEvent);
            event.setHh("1300");
            String asset = "NONASSET";
            if (i > 5)
                asset = "ASSET";
            event.setAssetType(asset);
            event.setTags(tags);
            event.setEnterpriseId("enterprise_GE");
            event.setMeasuresAggr(measuresAgg);
            event.setMeasuresAvg(measuresAvg);
            event.setExtProps(ext_props);
            event.setSiteId("siteId1");
            event.setYymmdd(dateEvent);
            eventList.add(event);
        }
        return eventList;
    }

    public List<Event> eventWithoutTagsRequired() {

        List<Event> eventList = new ArrayList<>();
        Event event = new Event();
        Map<String, String> ext_props = new HashMap<>();
        ext_props.put("asset_type", "rtu");
        ext_props.put("asset_type", "rtu");

        Map<String, Object> measuresAgg = new HashMap<>();
        measuresAgg.put("test1", "280");
        measuresAgg.put("test2", "268");

        Map<String, Object> measuresAvg = new HashMap<>();
        measuresAvg.put("test1", "70");
        measuresAvg.put("test2", "67");

        Map<String, String> tags = new HashMap<>();
        tags.put("runttime_on_stage1", "10");
        tags.put("runttime_on_stage2", "20");


        for (int i = 0; i < 5; i++) {
            event.setAssetId("");
            event.setHhmm(this.hhmmEvent);
            event.setHh("1300");
            event.setAssetType("rtu");
            event.setTags(tags);
            event.setEnterpriseId("enterprise_GE");
            event.setMeasuresAggr(measuresAgg);
            event.setMeasuresAvg(measuresAvg);
            event.setExtProps(ext_props);
            event.setSiteId("siteId1");
            event.setYymmdd(dateEvent);
            eventList.add(event);
        }

        return eventList;
    }

}
