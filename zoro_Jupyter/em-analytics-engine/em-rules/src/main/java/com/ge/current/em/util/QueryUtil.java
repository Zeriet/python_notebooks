package com.ge.current.em.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@Component public class QueryUtil {

    public static final Integer RESRC_UID_INDEX = 0;
    public static final Integer RESC_TYPE_INDEX = 1;
    public static final Integer EXT_PROPS_INDEX = 2;
    public static final Integer MEASURES_AVG_INDEX = 3;
    public static final Integer MEASURES_AGGR_INDEX = 4;
    public static final Integer EVENT_TS_TZ_INDEX = 4;
    public static final Integer TAGS_INDEX = 5;
    public static final Integer YYMMDD_INDEX = 6;
    public static final Integer SITE_UID = 6;
    public static final Integer HHMM_INDEX = 7;
    public static final Integer EVENT_TS_INDEX = 7;
    public static final Integer ENTERPRISE_UID_INDEX = 8;

    private static final String alertReturnParams = "time_bucket, resrc_uid, resrc_type,enterprise_uid, alert_ts, alert_ts_tz, log_uuid,  alert_type, alert_name,severity, category,duration, status, site_uid, site_name,zone_name,asset_uid,asset_type,ext_props";
    public static final Integer ALERT_LOG_TIME_BUCKET = 0;
    public static final Integer ALERT_LOG_RESRC_UID_INDEX = 1;
    public static final Integer ALERT_LOG_RESC_TYPE_INDEX = 2;
    public static final Integer ALERT_LOG_ENTERPRISE_UID_INDEX = 3;
    public static final Integer ALERT_LOG_ALERT_TS_INDEX = 4;
    public static final Integer ALERT_LOG_ALERT_TZ_LOC_INDEX = 5;
    public static final Integer ALERT_LOG_LOG_UUID_INDEX = 6;
    public static final Integer ALERT_LOG_ALERT_TYPE_INDEX = 7;
    public static final Integer ALERT_LOG_ALERT_NAME_INDEX = 8;
    public static final Integer ALERT_LOG_SEVERITY_INDEX = 9;
    public static final Integer ALERT_LOG_CATEGORY_INDEX = 10;
    public static final Integer ALERT_LOG_DURATION_INDEX = 11;
    public static final Integer ALERT_LOG_STATUS_INDEX = 12;
    public static final Integer ALERT_LOG_SITE_UID_INDEX = 13;
    public static final Integer ALERT_LOG_SITE_NAME_INDEX = 14;
    public static final Integer ALERT_LOG_ZONE_NAME_INDEX = 15;
    public static final Integer ALERT_LOG_ASSET_UID_INDEX = 16;
    public static final Integer ALERT_LOG_ASSET_TYPE_INDEX = 17;
    public static final Integer ALERT_LOG_EXT_PROPS_INDEX = 18;

    public static final String NORM_LOG_TABLENAME = "event_norm_log";
    public static final String EVENTS_BY_MIN_TABLENAME = "events_byminute_stg";

    private static final Logger LOG = LoggerFactory.getLogger(QueryUtil.class);

    public static String getQueryStringForTable(String keySpace, String tableName, String timeZone, String timeBucket,
            String startTime, String endTime) throws ParseException {
        String cql = "";
        if (EVENTS_BY_MIN_TABLENAME.equalsIgnoreCase(tableName)) {
            cql = getQueryFor15Mins(keySpace, tableName, timeZone, timeBucket);
        } else if (NORM_LOG_TABLENAME.equalsIgnoreCase(tableName)) {
            List<String> timeBuckets = getTimeBuckets(timeZone, startTime);

            cql = getQueryForNormalizedTable(keySpace, tableName, timeZone, timeBuckets);
        }
        return cql;
    }

    private static String getQueryFor15Mins(String keySpace, String tableName, String timeZone, String timeBucket)
            throws ParseException {
        String cql = "";
        cql = "SELECT resrc_uid, resrc_type, ext_props, measures_avg,measures_aggr, tags, yyyymmdd, hhmm, enterprise_uid   FROM "
                + " " + keySpace + "." + tableName + " where " + " event_bucket='" + timeZone + "' and"
                + " yyyymmdd in ('" + timeBucket + "')";
        LOG.warn("CQL: " + cql);
        return cql;
    }

    private static String getQueryForNormalizedTable(String keySpace, String tableName, String timeZone,
            List<String> timeBuckets) throws ParseException {
        String timeBucketsString = StringUtils.join(timeBuckets, "','");
        String cql = "";
        cql = "SELECT resrc_uid, resrc_type, ext_props, measures, event_ts_tz, tags,  site_uid, event_ts, enterprise_uid FROM "
                + " " + keySpace + "." + tableName + " where " + " time_bucket in ('" + timeBucketsString + "')";
        LOG.warn("CQL: " + cql);
        return cql;
    }

    public static String getAlarmsForPastRunQuery(String timeBucket, String keySpace, String tableName) {
        String cql = "";
        cql = "SELECT " + alertReturnParams + " FROM " + " " + keySpace + "." + tableName + " where "
                + " time_bucket in ('" + timeBucket + "')";
        LOG.warn("CQL: " + cql);
        return cql;
    }


    public static List<String> getTimeBuckets(String timeZone, String startTime) {
        List<String> timeBuckets = new ArrayList<>();
        int timeInterval = 15;
        timeBuckets.add(DateUtils.convertLocalTimeStrToUtcTimeStr(timeZone, startTime));
        timeBuckets.add(DateUtils
                .convertLocalTimeStrToUtcTimeStr(timeZone, DateUtils.addMinutesToDate(timeInterval, startTime)));
        timeBuckets.add(DateUtils
                .convertLocalTimeStrToUtcTimeStr(timeZone, DateUtils.addMinutesToDate(timeInterval * 2, startTime)));
        timeBuckets.add(DateUtils
                .convertLocalTimeStrToUtcTimeStr(timeZone, DateUtils.addMinutesToDate(timeInterval * 3, startTime)));
        return timeBuckets;
    }

}
