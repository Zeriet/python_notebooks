package com.ge.current.ie.analytics.batch;

import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.em.aggregation.dao.IECommonEvent;
import com.ge.current.em.aggregation.utils.AggregationUtil;
import com.ge.current.ie.analytics.batch.service.AggregationService;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.model.nosql.EventsDailyLog;
import com.ge.current.ie.model.nosql.EventsDailyStaging;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Map;

import static com.ge.current.ie.bean.SparkConfigurations.SPARK_APP_NAME;

/**
 * Created by 212582112 on 2/8/17.
 */

@Component
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class SparkCassandraDailyBatch implements ISparkBatch, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkCassandraDailyBatch.class);

    private Broadcast<Map<String, String>> segmentLoadTypeMappingBC = null;

    @Autowired
    transient APMDataLoader apmDataLoader;

    @Autowired
    PropertiesBean propertiesBean;

    @Override
    public void run(Environment env, String startTime, String endTime, String timeZone) {
        // @formatter: off
        AggregationService aggregationService = new AggregationService();

        SparkContext sc = aggregationService.getCassandraConfiguredSparkContext(getAppName(), getPropertiesBean());
        CassandraSQLContext cassandraSQLContext = new CassandraSQLContext(sc);

        broadcast(sc, getPropertiesBean());

        processDailyEvents(aggregationService, cassandraSQLContext, env, 
                           startTime, endTime, timeZone);

        // @formatter: on
    }

    public void processDailyEvents(AggregationService aggregationService, 
            CassandraSQLContext cassandraSQLContext, Environment env, 
            String startTime, String endTime, String timeZone)  
    {
        // Cache the RDD since it is used to derive EventsDailyLog RDD.
        JavaRDD<EventsDailyStaging> eventsDailyStaging = generateTimeZonedAggregations(
                aggregationService,
                cassandraSQLContext,
                env,
                startTime,
                endTime,
                timeZone).cache();

        JavaRDD<EventsDailyLog> eventsDailyLog = getEventsDailyLogFromEventsDailyStaging(eventsDailyStaging);

        // Write to cassandra
        writeToCassandra(aggregationService,
                         env,
                         eventsDailyLog,
                         eventsDailyStaging);
    }


    @Override
    public void broadcast(SparkContext sc, PropertiesBean propertiesBean) {
        Map<String, String> segmentLoadTypeMapping = apmDataLoader.getLoadTypeMapping();
        segmentLoadTypeMappingBC = sc.broadcast(segmentLoadTypeMapping, scala.reflect.ClassTag$.MODULE$.apply(segmentLoadTypeMapping.getClass()));
    }

    public void setMetaData(Broadcast<Map<String, String>> segmentLoadTypeMappingBC) {
        System.out.println("**** Setting Broadcast in Daily Job ****");
        this.segmentLoadTypeMappingBC = segmentLoadTypeMappingBC;
    }

    public JavaRDD<EventsDailyStaging> generateTimeZonedAggregations(AggregationService aggregationService,
                                                                     CassandraSQLContext cassandraSQLContext,
                                                                     Environment env,
                                                                     String localStartTime,
                                                                     String localEndTime,
                                                                     String localTimezone) {
        // @formatter: off
        // Build query
        String query = aggregationService.getHourlyAggregationsQuery(env.getProperty("cassandra.keyspace"),
                                                                     env.getProperty("cassandra.aggregation.table.eventshourlystg"),
                                                                     localStartTime,
                                                                     localEndTime,
                                                                     localTimezone);

        // Get all hourly aggregated events
        JavaRDD<IECommonEvent> hourlyAggregatedEvents = aggregationService.queryCassandra(cassandraSQLContext,
                query,
                AggregationUtil.byMinuteStagingToCommonEventMapper);

        // Generate Aggregations
        JavaRDD<EventsDailyStaging> dailyAggregatedEvents = aggregationService.aggregateEvents(hourlyAggregatedEvents)
                                                                              .map(commonEvent -> AggregationUtil.commonEventsToDailyStagingMapper(commonEvent,
                                                                                                                                                   localTimezone,
                                                                                                                                                   localStartTime.substring(0, 6),
                                                                                                                                                   Integer.valueOf(localStartTime.substring(6, 8))));

        // @formatter: on
        return dailyAggregatedEvents;
    }

    /**
     * Convert EventsDailyStaging to EventsDailyLog bean so the aggregations can be
     * persisted to events daily cassandra table.
     *
     * @param eventsDailyStaging RDD containing aggregations to be written to events daily staging table.
     * @return RDD containing aggregations to be written to events daily table.
     */
    public JavaRDD<EventsDailyLog> getEventsDailyLogFromEventsDailyStaging(JavaRDD<EventsDailyStaging> eventsDailyStaging) {
        return eventsDailyStaging.map(eventsDailyStg -> AggregationUtil.eventsDailyStagingToEventsDailyLogMapper(eventsDailyStg, segmentLoadTypeMappingBC.value()));
    }

    /**
     * Write the daily aggregations to events_daily and events_daily_stg
     * Cassandra tables
     *
     * @param aggregationService Aggregations service that does the actual reads and writes from Cassandra
     * @param env                Environment variable containing all the properties
     * @param eventsDailyLog     RDD containing aggregations to be written to events daily table
     * @param eventsDailyStaging RDD containing aggregations to be written to events daily staging table
     */
    public void writeToCassandra(AggregationService aggregationService,
                                 Environment env,
                                 JavaRDD<EventsDailyLog> eventsDailyLog,
                                 JavaRDD<EventsDailyStaging> eventsDailyStaging) {
        // @formatter: off
        aggregationService.writeToCassandra(eventsDailyLog,
                                            env.getProperty("cassandra.keyspace"),
                                            env.getProperty("cassandra.aggregation.table.eventsdaily"),
                                            EventsDailyLog.class);

        aggregationService.writeToCassandra(eventsDailyStaging,
                                            env.getProperty("cassandra.keyspace"),
                                            env.getProperty("cassandra.aggregation.table.eventsdailystg"),
                                            EventsDailyStaging.class);
        // @formatter: on
    }

    @Override
    public PropertiesBean getPropertiesBean() {
        return propertiesBean;
    }

    @Override
    public String getMaster() {
        return null;
    }

    @Override
    public String getAppName() {
        return propertiesBean.getValue(SPARK_APP_NAME) + "_DailySummaryBatchJob";
    }
}
