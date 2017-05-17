package com.ge.current.em.aggregation.utils;

/**
 * Created by 212582112 on 1/14/17.
 */
public class AggregationSQL {
    // @formatter:off
    public static final String MONTHLY_DATA_QUERY = "SELECT event_bucket, " +
                                                    "       resrc_type, " +
                                                    "       resrc_uid, " +
                                                    "       enterprise_uid," +
                                                    "       measures_aggr, " +
                                                    "       measures_cnt, " +
                                                    "       measures_min, " +
                                                    "       measures_max, " +
                                                    "       measures_avg, " +
                                                    "       tags," +
                                                    "       ext_props " +
                                                    "       yyyymm, " +
                                                    "       day, " +
                                                    "       log_uuid, " +
                                                    "       event_ts, " +
                                                    "       region_name, " +
                                                    "       site_city, " +
                                                    "       site_state, " +
                                                    "       site_country, " +
                                                    "       site_name, " +
                                                    "       zones, " +
                                                    "       segments, " +
                                                    "       labels " +
                                                    " FROM %s.%s " +
                                                    " WHERE " +
                                                    "       yyyymm='%s' AND " +
                                                    "       event_bucket='%s'";

    public static final String DAILY_DATA_QUERY = "SELECT event_bucket, " +
                                                   "      resrc_type, " +
                                                   "      resrc_uid, " +
                                                   "      enterprise_uid," +
                                                   "      measures_aggr, " +
                                                   "      measures_cnt, " +
                                                   "      measures_min, " +
                                                   "      measures_max, " +
                                                   "      measures_avg, " +
                                                   "      tags," +
                                                   "      ext_props " +
                                                   "      yyyymmdd, " +
                                                   "      hour, " +
                                                   "      log_uuid, " +
                                                   "      event_ts, " +
                                                   "      region_name, " +
                                                   "      site_city, " +
                                                   "      site_state, " +
                                                   "      site_country, " +
                                                   "      site_name, " +
                                                   "      zones, " +
                                                   "      segments, " +
                                                   "      labels " +
                                                   " FROM %s.%s " +
                                                   " WHERE " +
                                                   "      yyyymmdd='%s' AND " +
                                                   "      event_bucket='%s'";

    public static final String HOURLY_DATA_QUERY = "SELECT event_bucket, " +
                                                   "       resrc_type," +
                                                   "       resrc_uid," +
                                                   "       enterprise_uid," +
                                                   "       measures_aggr, " +
                                                   "       measures_cnt, " +
                                                   "       measures_min, " +
                                                   "       measures_max, " +
                                                   "       measures_avg, " +
                                                   "       tags," +
                                                   "       ext_props " +
                                                   "       yyyymmdd, " +
                                                   "       hhmm, " +
                                                   "       increment_mins, " +
                                                   "       log_uuid, " +
                                                   "       event_ts, " +
                                                   "       region_name, " +
                                                   "       site_city, " +
                                                   "       site_state, " +
                                                   "       site_country, " +
                                                   "       site_name, " +
                                                   "       zones, " +
                                                   "       segments, " +
                                                   "       labels " +
                                                   " FROM %s.%s " +
                                                   " WHERE " +
                                                   "       yyyymmdd='%s' AND " +
                                                   "       event_bucket='%s'";
    // @formatter:on


    public static class FieldIndex {
        public static final int BYMINUTE_STG_TZ_IDX = 0;
        public static final int BYMINUTE_STG_RESRC_TYPE_IDX = 1;
        public static final int BYMINUTE_STG_RESRC_UID_IDX = 2;
        public static final int BYMINUTE_STG_ENT_UID_IDX = 3;

        public static final int BYMINUTE_STG_MEASURES_AGGR_IDX = 4;
        public static final int BYMINUTE_STG_MEASURES_CNT_IDX = 5;
        public static final int BYMINUTE_STG_MEASURES_MIN_IDX = 6;
        public static final int BYMINUTE_STG_MEASURES_MAX_IDX = 7;
        public static final int BYMINUTE_STG_MEASURES_AVG_IDX = 8;
        public static final int BYMINUTE_STG_TAGS_IDX = 9;
        public static final int BYMINUTE_STG_EXT_PROPS_IDX = 10;
    }
}
