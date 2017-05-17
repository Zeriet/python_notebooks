package com.ge.current.ie.analytics.batch;

import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.em.aggregation.dao.IECommonEvent;
import com.ge.current.em.aggregation.utils.AggregationUtil;
import com.ge.current.ie.analytics.batch.service.AggregationService;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.model.nosql.EventsMonthlyLog;
import com.ge.current.ie.model.nosql.EventsMonthlyStaging;
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
 * Created by 212582112 on 2/9/17.
 */

@Component
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class SparkCassandraMonthlyBatch implements ISparkBatch, Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkCassandraMonthlyBatch.class);

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

        processMonthly(aggregationService, cassandraSQLContext, env, startTime, endTime, timeZone);

    }

    public void processMonthly(AggregationService aggregationService,
            CassandraSQLContext cassandraSQLContext, Environment env, 
            String startTime, String endTime, String localTimezone) 
    {
        JavaRDD<EventsMonthlyStaging> eventsMonthlyStaging = generateTimeZonedAggregations(
                      aggregationService,
                      cassandraSQLContext,
                      env,
                      startTime,
                      endTime,
                      localTimezone).cache();

        JavaRDD<EventsMonthlyLog> eventsMonthlyLog = getEventsMonthlyLogFromEventsMonthlyStaging(eventsMonthlyStaging);

        // Write to cassandra
        writeToCassandra(aggregationService,
                         env,
                         eventsMonthlyLog,
                         eventsMonthlyStaging);
        // @formatter: on
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

    public JavaRDD<EventsMonthlyStaging> generateTimeZonedAggregations(AggregationService aggregationService,
                                                                       CassandraSQLContext cassandraSQLContext,
                                                                       Environment env,
                                                                       String localStartTime,
                                                                       String localEndTime,
                                                                       String localTimezone) {
        // @formatter: off
        // Build query
        String query = aggregationService.getDailyAggregationsQuery(env.getProperty("cassandra.keyspace"),
                                                                    env.getProperty("cassandra.aggregation.table.eventsdailystg"),
                                                                    localStartTime,
                                                                    localEndTime,
                                                                    localTimezone);

        // Get all daily aggregated events
        JavaRDD<IECommonEvent> dailyAggregatedEvents = aggregationService.queryCassandra(cassandraSQLContext,
                                                                                         query,
                                                                                         AggregationUtil.byMinuteStagingToCommonEventMapper);

        // Generate Aggregations
        JavaRDD<EventsMonthlyStaging> monthlyAggregatedEvents = aggregationService.aggregateEvents(dailyAggregatedEvents)
                                                                                  .map(commonEvent -> AggregationUtil.commonEventsToMonthlyStagingMapper(commonEvent,
                                                                                                                                                         localTimezone,
                                                                                                                                                         Integer.valueOf(localStartTime.substring(0, 4)),
                                                                                                                                                         Integer.valueOf(localStartTime.substring(4, 6))));

        return monthlyAggregatedEvents;
        // @formatter: on
    }

    /**
     * Convert EventsMonthlyStaging to EventsMonthlyLog bean so the aggregations can be
     * persisted to events daily cassandra table.
     *
     * @param eventsMonthlyStaging RDD containing aggregations to be written to events daily staging table.
     * @return RDD containing aggregations to be written to events daily table.
     */
    public JavaRDD<EventsMonthlyLog> getEventsMonthlyLogFromEventsMonthlyStaging(JavaRDD<EventsMonthlyStaging> eventsMonthlyStaging) {
        return eventsMonthlyStaging.map(eventsMonthlyStg -> AggregationUtil.eventsMonthlyStagingToEventsMonthlyLogMapper(eventsMonthlyStg, segmentLoadTypeMappingBC.value()));
    }

    /**
     * Write the monthly aggregations to events_monthly and events_monthly_stg
     * Cassandra tables
     *
     * @param aggregationService   Aggregations service that does the actual reads and writes from Cassandra
     * @param env                  Environment variable containing all the properties
     * @param eventsMonthlyLog     RDD containing aggregations to be written to events monthly table
     * @param eventsMonthlyStaging RDD containing aggregations to be written to events monthly staging table
     */
    public void writeToCassandra(AggregationService aggregationService,
                                 Environment env,
                                 JavaRDD<EventsMonthlyLog> eventsMonthlyLog,
                                 JavaRDD<EventsMonthlyStaging> eventsMonthlyStaging) {
        // @formatter: off
        aggregationService.writeToCassandra(eventsMonthlyLog,
                                            env.getProperty("cassandra.keyspace"),
                                            env.getProperty("cassandra.aggregation.table.eventsmonthly"),
                                            EventsMonthlyLog.class);

        aggregationService.writeToCassandra(eventsMonthlyStaging,
                                            env.getProperty("cassandra.keyspace"),
                                            env.getProperty("cassandra.aggregation.table.eventsmonthlystg"),
                                            EventsMonthlyStaging.class);
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
        return propertiesBean.getValue(SPARK_APP_NAME) + "_MonthlySummaryBatchJob";
    }
}
