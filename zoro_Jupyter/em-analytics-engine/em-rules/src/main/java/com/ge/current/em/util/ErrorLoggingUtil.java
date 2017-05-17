package com.ge.current.em.util;

import com.ge.current.em.entities.analytics.MessageLog;
import com.ge.current.ie.bean.SparkConfigurations;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.ie.bean.PropertiesBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by 212554696 on 4/14/17.
 */
public class ErrorLoggingUtil {

    public static final String APP_NAME = "Rule-Engine-Spark";

    public static final String BEFORE_FILTER_COUNT = "tagsbeforeFilterCount";
    public static final String AFTER_FILTER_COUNT = "tagsafterFilterCount";
    public static final String TOTAL_PAST_ALERTS = "tagstotalPastAlerts";
    public static final String TOTAL_PRESENT_ALERTS = "tagstotalPresentAlerts";
    public static final String TOTAL_ALERTS_TO_BE_SAVED = "tagstotalAlertsToBeSaved";
    public static final String TOTAL_SAVED_ALERTS = "tagstotalSavedAlerts";

    public static final String APP_KEY_NAME = "em-rules-engine";

    private static final Logger LOG = LoggerFactory.getLogger(ErrorLoggingUtil.class);

    public static void writeErrorsToCassandra(PropertiesBean propertiesBean, JavaRDD<MessageLog> messageLog) {
        javaFunctions(messageLog).writerBuilder(propertiesBean.getValue(SparkConfigurations.CASSANDRA_KEYSPACE),
                "error_log", mapToRow(MessageLog.class)).saveToCassandra();
    }

    public static MessageLog createMessageLog(String ruleNames[], String timeBucket, String timeZone, String messageSummary, Map<String, String> tags) {

        MessageLog messageLog = new MessageLog();
        messageLog.setTimeBucket(timeBucket);
        messageLog.setProcessUid(UUID.randomUUID().toString());

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
        Date currentDate = calendar.getTime();
        messageLog.setErrorTs(currentDate);

        UUID timeBasedUuid = UUIDs.timeBased();
        messageLog.setLogUUID(timeBasedUuid);

        messageLog.setAppKey(APP_KEY_NAME);

        // skipping category, dest_uri, job_uid, msg, msg_ext_uri

        messageLog.setMsgSumry(messageSummary);
        String ruleNamesInString = StringUtils.join(ruleNames, ",");
        messageLog.setMsgUID(ruleNamesInString + '-' + timeZone);

        // skipping origin_uri, solr_query
        Map<String, String> tagsMap = new HashMap<>();
        messageLog.setTags(tags);
        return messageLog;
    }
}

