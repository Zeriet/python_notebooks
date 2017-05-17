package com.ge.current.ie.analytics.batch;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.ge.current.em.aggregation.dao.IECommonEvent;
import com.ge.current.em.aggregation.utils.AggregationUtil;
import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.em.analytics.common.EmailService;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.em.entities.analytics.APMMappings;
import com.ge.current.ie.analytics.batch.service.AggregationService;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.model.nosql.EventsHourlyLog;
import com.ge.current.ie.model.nosql.EventsHourlyStaging;

import static com.ge.current.ie.bean.SparkConfigurations.SPARK_APP_NAME;

@Component
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class SparkCassandraHourlyBatch implements ISparkBatch, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SparkCassandraHourlyBatch.class);

    @Autowired
    PropertiesBean propertiesBean;

    @Autowired
    transient APMDataLoader apmDataLoader;

    @Autowired
    transient EmailService emailService;

    private Broadcast<Map<String, String>> segmentLoadTypeMappingBC = null;
    
    @Autowired
    SparkCassandraKPITillDateAggregation kpiCalculation;
    private Broadcast<Map<String, HashMap<String, Integer[]>>> siteOccupancyMappingBC = null;
    private Broadcast<Map<String, String>> siteTimeZoneMappingBC = null;
    private Broadcast<HashSet<String>> allLoadTypeSegmentsBC = null;
    private Broadcast<Map<String, Double>> siteSqftMappingBC = null;
    private Broadcast<Map<String, Integer>> siteInstallationYearsMappingBC = null;
    private Broadcast<Map<String, HashMap<Integer, String>>> sitePeakHoursMappingBC = null;
    private Broadcast<Map<String, String>> segmentSiteMappingBC = null;
    private Broadcast<Map<String, String>> segmentEnterpriseMappingBC = null;
    private Broadcast<Map<String, String>> siteEnterpriseMappingBC = null;
    private Broadcast<Map<String, String>> loadTypeSegmentNameMappingBC = null;

    @Override
    public void run(Environment env, String startTime, String endTime, String timeZone) {
        // @formatter: off
        AggregationService aggregationService = new AggregationService();

        SparkContext sc = aggregationService.getCassandraConfiguredSparkContext(getAppName(), getPropertiesBean());
        CassandraSQLContext cassandraSQLContext = new CassandraSQLContext(sc);

        broadcast(sc, getPropertiesBean());

        processHourlyEvents(sc, aggregationService, cassandraSQLContext, 
                kpiCalculation, env, 
                startTime, endTime, env.getProperty("cassandra.aggregation.localtimezones"));
                //startTime, endTime, "Australia/Melbourne"); 
    }

    public void processHourlyEvents(SparkContext sc, AggregationService aggregationService, 
            CassandraSQLContext cassandraSQLContext, 
            SparkCassandraKPITillDateAggregation kpiCalculationObj, Environment env,
            String startTime, String endTime, String timezones) {

        // Explicitly cache the hourly staging RDD
        JavaRDD<EventsHourlyStaging> eventsHourlyStaging = generateAllTimeZoneAggregations(env,
                   aggregationService,
                   cassandraSQLContext,
                   startTime,
                   endTime,
                   timezones)
                   .cache();

        JavaRDD<EventsHourlyLog> eventsHourlyLog = getEventsHourlyLogFromEventsHourlyStaging(eventsHourlyStaging);

        // Write to cassandra
        writeToCassandra(aggregationService,
                         env,
                         eventsHourlyLog,
                         eventsHourlyStaging);

        System.out.println("**** Hourly job is done, continue to KPI ****");

        // Pass the RDD and Cassandra Context to KPI job
        APMMappings apmMappings = new APMMappings(allLoadTypeSegmentsBC.getValue(),
                                                  siteSqftMappingBC.getValue(),
                                                  siteOccupancyMappingBC.getValue(),
                                                  siteTimeZoneMappingBC.getValue(),
                                                  siteInstallationYearsMappingBC.getValue(),
                                                  sitePeakHoursMappingBC.getValue(),
                                                  segmentSiteMappingBC.getValue(),
                                                  segmentEnterpriseMappingBC.getValue(),
                                                  siteEnterpriseMappingBC.getValue(),
                                                  loadTypeSegmentNameMappingBC.getValue());

         kpiCalculationObj.tillDateKpiBatch(eventsHourlyLog, apmMappings, sc, env.getProperty("cassandra.keyspace"));
        // @formatter: on
    }

    @Override
    public void broadcast(SparkContext sc, PropertiesBean propertiesBean) {
        JavaSparkContext jsc = new JavaSparkContext(sc);

        try {
            apmDataLoader.generateMappings();
        } catch (Exception ex) {
            Map<String, String> configErrorMap = apmDataLoader.getConfigErrorMap();
             emailService.sendMessage("EM Hourly Aggregation Spark Job",
                      sc.applicationId(), configErrorMap, ex,
                      "EM Hourly Aggregation Job broadcast Alert.");
        }

        segmentLoadTypeMappingBC = jsc.broadcast(apmDataLoader.getLoadTypeMapping());
        siteOccupancyMappingBC = jsc.broadcast(apmDataLoader.generateSiteOccupancyMapping());
        siteTimeZoneMappingBC = jsc.broadcast(apmDataLoader.generateSiteTimeZoneMapping());
        allLoadTypeSegmentsBC = jsc.broadcast(new HashSet<>(apmDataLoader.getLoadTypeMapping().keySet()));
        siteSqftMappingBC = jsc.broadcast(apmDataLoader.generateSiteSqftMapping());
        siteInstallationYearsMappingBC = jsc.broadcast(apmDataLoader.generateSiteInstallationYearMapping());
        sitePeakHoursMappingBC = jsc.broadcast(apmDataLoader.generateSitePeakHoursMapping());
        segmentSiteMappingBC = jsc.broadcast(apmDataLoader.generateSegmentSiteMapping());
        segmentEnterpriseMappingBC = jsc.broadcast(apmDataLoader.generateSegmentEnterpriseMapping());
        siteEnterpriseMappingBC = jsc.broadcast(apmDataLoader.generateSiteEnterpriseMapping());
        loadTypeSegmentNameMappingBC = jsc.broadcast(apmDataLoader.getLoadTypeSegmentNameMapping());
    }

    public void setMetaData(Broadcast<Map<String, String>> segmentLoadTypeMappingBC, 
            Broadcast<Map<String, HashMap<String, Integer[]>>> siteOccupancyMappingBC,
            Broadcast<Map<String, String>> siteTimeZoneMappingBC,
            Broadcast<Map<String, Double>> siteSqftMappingBC,
            Broadcast<Map<String, Integer>> siteInstallationYearsMappingBC,
            Broadcast<Map<String, HashMap<Integer, String>>> sitePeakHoursMappingBC,
            Broadcast<Map<String, String>> segmentSiteMappingBC,
            Broadcast<Map<String, String>> segmentEnterpriseMappingBC,
            Broadcast<Map<String, String>> siteEnterpriseMappingBC,
            Broadcast<Map<String, String>> loadTypeSegmentNameMappingBC,
            Broadcast<HashSet<String>> allLoadTypeSegmentsBC,
            PropertiesBean propertiesBean) {
        System.out.println("**** Setting Broadcast in Hourly Job ****");
        this.segmentLoadTypeMappingBC = segmentLoadTypeMappingBC;
        this.siteOccupancyMappingBC = siteOccupancyMappingBC;
        this.siteTimeZoneMappingBC = siteTimeZoneMappingBC;
        this.siteSqftMappingBC = siteSqftMappingBC;
        this.siteInstallationYearsMappingBC = siteInstallationYearsMappingBC;
        this.sitePeakHoursMappingBC = sitePeakHoursMappingBC;
        this.segmentSiteMappingBC = segmentSiteMappingBC;
        this.segmentEnterpriseMappingBC = segmentEnterpriseMappingBC;
        this.siteEnterpriseMappingBC = siteEnterpriseMappingBC;
        this.loadTypeSegmentNameMappingBC = loadTypeSegmentNameMappingBC;
        this.allLoadTypeSegmentsBC = allLoadTypeSegmentsBC;
        this.propertiesBean = propertiesBean;
    }

    /**
     * @param env
     * @param aggregationService
     * @param cassandraSQLContext
     * @param startTime
     * @param endTime
     * @param localTimezones
     * @return
     */
    public JavaRDD<EventsHourlyStaging> generateAllTimeZoneAggregations(Environment env,
                                                                        AggregationService aggregationService,
                                                                        CassandraSQLContext cassandraSQLContext,
                                                                        String startTime,
                                                                        String endTime,
                                                                        String localTimezones) {
        JavaRDD<EventsHourlyStaging> mergedEventsHourlyStaging = null;
        for (String localTimezone : localTimezones.split(",")) {
            try {
                String localStartTime = DateUtils.convertUTCStringToLocalTimeString(startTime, "yyyyMMddHH", localTimezone, "yyyyMMddHH");
                String localEndTime = DateUtils.convertUTCStringToLocalTimeString(endTime, "yyyyMMddHH", localTimezone, "yyyyMMddHH");
                System.out.println("Local start time = " + localStartTime + 
                        "  local end time = " + localEndTime);

                JavaRDD<EventsHourlyStaging> timeZonedAggregations = generateTimeZonedAggregations(aggregationService,
                                                                                                   cassandraSQLContext,
                                                                                                   env,
                                                                                                   localStartTime,
                                                                                                   localEndTime,
                                                                                                   localTimezone);

                // Generate one final RDD containing
                // aggregations for all timezones.
                if (mergedEventsHourlyStaging == null) {
                    mergedEventsHourlyStaging = timeZonedAggregations;
                    System.out.println("*** timezone = " + localTimezone);
                } else {
                    mergedEventsHourlyStaging = mergedEventsHourlyStaging.union(timeZonedAggregations);
                    System.out.println("*** Union timezone = " + localTimezone);
                }
            } catch (Exception e) {
                // todo: send an email notification since this is a very rare event.
                LOG.error("Failed to process hourly aggregation. Error: ", e);
            }
            //System.out.println("*** Merged size = " + mergedEventsHourlyStaging.collect().size());
        }

        return mergedEventsHourlyStaging;
    }

    /**
     * Generate hourly aggregations for a single time zone.
     *
     * @param aggregationService
     * @param cassandraSQLContext
     * @param env
     * @param localStartTime
     * @param localEndTime
     * @param localTimezone
     * @return
     */
    public JavaRDD<EventsHourlyStaging> generateTimeZonedAggregations(AggregationService aggregationService,
                                                                      CassandraSQLContext cassandraSQLContext,
                                                                      Environment env,
                                                                      String localStartTime,
                                                                      String localEndTime,
                                                                      String localTimezone) {
        // @formatter: off
        String yyyymmdd = localStartTime.substring(0, 8);
        String hour = localStartTime.substring(8, 10);

        // Build query
        String query = aggregationService.getEventsByMinuteStagingQuery(env.getProperty("cassandra.keyspace"),
                                                                        env.getProperty("cassandra.aggregation.table.eventsbyminstg"),
                                                                        yyyymmdd,
                                                                        yyyymmdd,
                                                                        localTimezone);

        
        // Get all 15 mins aggregated events
        JavaRDD<IECommonEvent> byminuteAggregatedEvents = aggregationService.queryCassandraWithHourlyFilter(cassandraSQLContext,
                                                                                                            query,
                                                                                                            hour,
                                                                                                            AggregationUtil.byMinuteStagingToCommonEventMapper);

        
        // Generate Aggregations
        JavaRDD<EventsHourlyStaging> hourlyAggregatedEvents = aggregationService
                    .aggregateEvents(byminuteAggregatedEvents)
                    .map(commonEvent -> AggregationUtil.commonEventsToHourlyStagingMapper(commonEvent,
                                                                                                                                                      localTimezone,
                                                                                                                                                      yyyymmdd,
                                                                                                                                                      hour));

        // @formatter: on
        return hourlyAggregatedEvents;
    }

    /**
     * Convert EventsHourlyStaging to EventsHourlyLog bean so the aggregations can be
     * persisted to events hourly cassandra table.
     *
     * @param eventsHourlyStaging RDD containing aggregations to be written to events hourly staging table.
     * @return RDD containing aggregations to be written to events hourly table.
     */
    public JavaRDD<EventsHourlyLog> getEventsHourlyLogFromEventsHourlyStaging(JavaRDD<EventsHourlyStaging> eventsHourlyStaging) {
        return eventsHourlyStaging.map(eventsHourlyStg -> AggregationUtil.eventsHourlyStagingToEventsHourlyMapper(eventsHourlyStg, segmentLoadTypeMappingBC.value()));
    }

    /**
     * Write the hourly aggregations to events_hourly and events_hourly_stg
     * Cassandra tables
     *
     * @param aggregationService  Aggregations service that does the actual reads and writes from Cassandra
     * @param env                 Environment variable containing all the properties
     * @param eventsHourlyLog     RDD containing aggregations to be written to events hourly table
     * @param eventsHourlyStaging RDD containing aggregations to be written to events hourly staging table
     */
    public void writeToCassandra(AggregationService aggregationService,
                                 Environment env,
                                 JavaRDD<EventsHourlyLog> eventsHourlyLog,
                                 JavaRDD<EventsHourlyStaging> eventsHourlyStaging) {

        eventsHourlyStaging.collect().stream().forEach(System.out::println);
        eventsHourlyLog.collect().stream().forEach(System.out::println);
        
        // @formatter: off
        aggregationService.writeToCassandra(eventsHourlyStaging,
                                            env.getProperty("cassandra.keyspace"),
                                            env.getProperty("cassandra.aggregation.table.eventshourlystg"),
                                            EventsHourlyStaging.class);

        aggregationService.writeToCassandra(eventsHourlyLog,
                                            env.getProperty("cassandra.keyspace"),
                                            env.getProperty("cassandra.aggregation.table.eventshourly"),
                                            EventsHourlyLog.class);
        // @formatter: on
    }

    @Override
    public PropertiesBean getPropertiesBean() {
        return propertiesBean;
    }

    @Override
    public String getAppName() {
        return propertiesBean.getValue(SPARK_APP_NAME) + "_HourlySummaryBatchJob";
    }

    @Override
    public String getMaster() {
        return null;
    }
}
