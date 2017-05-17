package com.ge.current.em.aggregation.broadcast;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;
import java.util.Map;

/**
 * Created by 212582112 on 2/21/17.
 */
public class EntityStatusManager {
    private static final Logger LOG = Logger.getLogger(EntityStatusManager.class);

    public static boolean entityUpdated(SQLContext sqlContext, Map<String, String> options, String entityName, boolean capitalizeFieldNames) {
        try {
            DataFrame preparedQuery = sqlContext.read().format("jdbc").options(options).load();
            preparedQuery.show();
            preparedQuery.registerTempTable("process_status");
            preparedQuery.persist();
        } catch (Exception e) {
            LOG.error("Failed to read process_status table. Reason: ", e);
            return false;
        }

        String preparedQuery = "SELECT status FROM process_status WHERE entity_name=\"%s\"";
        if(capitalizeFieldNames) {
            preparedQuery = "SELECT STATUS FROM process_status WHERE ENTITY_NAME=\"%s\"";
        }

        DataFrame results = sqlContext.sql(String.format(preparedQuery, entityName));
        List<Boolean> updated = results.javaRDD().map(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return (Boolean) row.get(0);
            }
        }).collect();
        return (updated.size() > 0)? updated.get(0) : false;
    }

    public static void resetEntityStatus(SQLContext sqlContext, Map<String, String> options, String entityName) {

    }
}
