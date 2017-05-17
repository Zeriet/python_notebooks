package com.ge.current.ie.analytics.batch.service;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.aggregation.dao.IECommonEvent;
import com.ge.current.em.aggregation.utils.AggregationSQL;
import com.ge.current.em.aggregation.utils.AggregationUtil;
import com.ge.current.em.aggregation.utils.ConnectionUtil;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.model.nosql.EventsHourlyLog;
import com.ge.current.ie.model.nosql.EventsHourlyStaging;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by 212582112 on 2/2/17.
 */

// Do not AUTOWIRE this class since it will cause
// the following expection
// Caused by: java.io.NotSerializableException: org.springframework.context.annotation.ConfigurationClassEnhancer$BeanMethodInterceptor
public class AggregationService implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(AggregationService.class);

    /**
     * Creates a spark context and populates cassandra connection information
     *
     * @param appName        Name of the Spark App
     * @param propertiesBean Bean containing cassandra connection information.
     * @return Configured Spark context
     */
    public SparkContext getCassandraConfiguredSparkContext(String appName, PropertiesBean propertiesBean) {
        // Create conf and setup Cassandra connection information
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        sparkConf = ConnectionUtil.configureCassandraConnection(sparkConf, propertiesBean);

        // Create Spark and Cassandra context
        return new SparkContext(sparkConf);
    }

    /**
     * Returns the cassandra query to fetch all the rows
     * corresponding for a given hour from the events_byminute_stg table
     *
     * @param keySpace  Cassandra keyspace
     * @param tableName Cassandra staging Events by minute table name
     * @param startTime The starting hour to fetch the data
     * @param endTime   The ending hour to fetch the data
     * @param timeZone  The timezone for which to fetch the data.
     * @return Cassandra query to fetch all the rows
     * corresponding to the given hour from the events_byminute_stg table
     */
    public String getEventsByMinuteStagingQuery(String keySpace,
                                                String tableName,
                                                String startTime,
                                                String endTime,
                                                String timeZone) {

        // @formatter: off
        String query = String.format(AggregationSQL.HOURLY_DATA_QUERY,
                                     keySpace,
                                     tableName,
                                     startTime,
                                     timeZone);
        // @formatter: on

        LOGGER.error("Cassandra byminute aggregated events query: " + query);
        return query;
    }

    /**
     * Returns the cassandra query to fetch all the rows
     * corresponding for a given day from the events_houlry_stg table
     *
     * @param keySpace  Cassandra keyspace
     * @param tableName Cassandra table name
     * @param startTime The starting hour to fetch the data
     * @param endTime   The ending hour to fetch the data
     * @param timeZone  The timezone for which to fetch the data.
     * @return Cassandra query to fetch all the rows
     * corresponding to the given hour from the events_byminute_stg table
     */
    public String getHourlyAggregationsQuery(String keySpace,
                                             String tableName,
                                             String startTime,
                                             String endTime,
                                             String timeZone) {
        // @formatter: off
        return getAggregationQuery(AggregationSQL.DAILY_DATA_QUERY,
                                   keySpace,
                                   tableName,
                                   startTime,
                                   endTime,
                                   timeZone);
        // @formatter: on
    }

    public String getDailyAggregationsQuery(String keySpace,
                                            String tableName,
                                            String startTime,
                                            String endTime,
                                            String timeZone) {
        // @formatter: off
        return getAggregationQuery(AggregationSQL.MONTHLY_DATA_QUERY,
                                   keySpace,
                                   tableName,
                                   startTime,
                                   endTime,
                                   timeZone);
        // @formatter: on
    }

    private String getAggregationQuery(String parameterizedQuery,
                                       String keySpace,
                                       String tableName,
                                       String startTime,
                                       String endTime,
                                       String timeZone) {
        // @formatter: off
        String query = String.format(parameterizedQuery,
                                     keySpace,
                                     tableName,
                                     startTime,
                                     timeZone);
        // @formatter: on

        LOGGER.error("Cassandra query: " + query);
        return query;
    }

    public JavaRDD<IECommonEvent> queryCassandra(CassandraSQLContext cassandraSQLContext,
                                                 String query,
                                                 Function<Row, IECommonEvent> rowToIECommentMapper) {
        // @formatter: off
        return cassandraSQLContext.cassandraSql(query)
                                  .toJavaRDD()
                                  .map(row -> rowToIECommentMapper.call(row))
                                  .filter(ieCommonEvent -> ieCommonEvent != null);
        // @formatter: on
    }

    public JavaRDD<IECommonEvent> queryCassandraWithHourlyFilter(CassandraSQLContext cassandraSQLContext,
                                                                 String query,
                                                                 String hour,
                                                                 Function<Row, IECommonEvent> rowToIECommentMapper) {
        String hourlySqlWildcard = hour + "%";

        // @formatter: off
        DataFrame df = cassandraSQLContext.cassandraSql(query);
        JavaRDD<IECommonEvent> commonEvents =  df.filter(df.col("hhmm").like(hourlySqlWildcard))
                                                 .toJavaRDD()
                                                 .map(row -> rowToIECommentMapper.call(row))
                                                 .filter(ieCommonEvent -> ieCommonEvent != null);

        return commonEvents;
        // @formatter: on
    }

    public JavaRDD<IECommonEvent> aggregateEvents(JavaRDD<IECommonEvent> microAggregations) {
        // @formatter: off
        return microAggregations.mapToPair(microAgg -> new Tuple2<>(microAgg.resourceId, microAgg))
                                .reduceByKey(AggregationUtil.aggregateCommonEvents)
                                .map(aggTuple -> aggTuple._2);
        // @formatter: on
    }

    public <T> void writeToCassandra(JavaRDD<T> events, String keyspace, String tableName, Class<T> className) {
        // @formatter: off
        javaFunctions(events).writerBuilder(keyspace,
                                            tableName,
                                            mapToRow(className))
                             .saveToCassandra();
        // @formatter: on
    }
}