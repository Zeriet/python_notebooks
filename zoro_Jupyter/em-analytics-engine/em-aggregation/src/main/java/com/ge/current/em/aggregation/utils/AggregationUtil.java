package com.ge.current.em.aggregation.utils;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.aggregation.dao.IECommonEvent;
import com.ge.current.em.analytics.common.*;
import com.ge.current.ie.model.nosql.*;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 212582112 on 2/3/17.
 */
public class AggregationUtil {
    private static final Logger LOGGER = Logger.getLogger(AggregationUtil.class);

    public static EventsMonthlyStaging commonEventsToMonthlyStagingMapper(IECommonEvent commonEvent,
                                                                          String timeZone,
                                                                          int yyyy,
                                                                          int month) {

        EventsMonthlyStaging monthlyStaging = new EventsMonthlyStaging();

        monthlyStaging.setEvent_bucket(timeZone);
        monthlyStaging.setYyyy(yyyy);
        monthlyStaging.setMonth(month);
        monthlyStaging.setResrc_type(commonEvent.resourceType);
        monthlyStaging.setResrc_uid(commonEvent.resourceId);
        monthlyStaging.setLog_uuid(UUIDs.timeBased());

        try {
            monthlyStaging.setEvent_ts(com.ge.current.em.analytics.common.DateUtils.getDateFromTimeStringTimeZone(yyyy + "" + month , "yyyyMM", timeZone));
        }catch (Exception e) {
            LOGGER.error("Failed to populate the event ts.", e);
        }

        monthlyStaging.setEnterprise_uid(commonEvent.enterpriseId);
        monthlyStaging.setRegion_name(commonEvent.regionName);
        monthlyStaging.setSite_city(commonEvent.siteCity);
        monthlyStaging.setSite_state(commonEvent.siteState);
        monthlyStaging.setSite_country(commonEvent.siteCountry);
        monthlyStaging.setSite_name(commonEvent.siteName);
        monthlyStaging.setZones(commonEvent.zones);
        monthlyStaging.setSegments(commonEvent.segments);
        monthlyStaging.setLabels(commonEvent.labels);

        // Measures
        monthlyStaging.setMeasures_aggr(commonEvent.measures_aggr);
        monthlyStaging.setMeasures_avg(commonEvent.measures_avg);
        monthlyStaging.setMeasures_max(commonEvent.measures_max);
        monthlyStaging.setMeasures_min(commonEvent.measures_min);
        monthlyStaging.setMeasures_cnt(commonEvent.measures_cnt);

        // Tags
        monthlyStaging.setTags(commonEvent.tags);

        // Ext Props
        monthlyStaging.setExt_props(commonEvent.ext_props);

        return monthlyStaging;
    }

    public static EventsDailyStaging commonEventsToDailyStagingMapper(IECommonEvent commonEvent,
                                                                      String timeZone,
                                                                      String yyyymm,
                                                                      int day) {

        EventsDailyStaging dailyStaging = new EventsDailyStaging();

        dailyStaging.setEvent_bucket(timeZone);
        dailyStaging.setYyyymm(yyyymm);
        dailyStaging.setDay(day);
        dailyStaging.setResrc_type(commonEvent.resourceType);
        dailyStaging.setResrc_uid(commonEvent.resourceId);
        dailyStaging.setLog_uuid(UUIDs.timeBased());

        try {
            dailyStaging.setEvent_ts(com.ge.current.em.analytics.common.DateUtils.getDateFromTimeStringTimeZone(yyyymm + day, "yyyyMMdd", timeZone));
        }catch (Exception e) {
            LOGGER.error("Failed to populate the event ts.", e);
        }

        dailyStaging.setEnterprise_uid(commonEvent.enterpriseId);
        dailyStaging.setRegion_name(commonEvent.regionName);
        dailyStaging.setSite_city(commonEvent.siteCity);
        dailyStaging.setSite_state(commonEvent.siteState);
        dailyStaging.setSite_country(commonEvent.siteCountry);
        dailyStaging.setSite_name(commonEvent.siteName);
        dailyStaging.setZones(commonEvent.zones);
        dailyStaging.setSegments(commonEvent.segments);
        dailyStaging.setLabels(commonEvent.labels);

        // Measures
        dailyStaging.setMeasures_aggr(commonEvent.measures_aggr);
        dailyStaging.setMeasures_avg(commonEvent.measures_avg);
        dailyStaging.setMeasures_max(commonEvent.measures_max);
        dailyStaging.setMeasures_min(commonEvent.measures_min);
        dailyStaging.setMeasures_cnt(commonEvent.measures_cnt);

        // Tags
        dailyStaging.setTags(commonEvent.tags);

        // Ext Props
        dailyStaging.setExt_props(commonEvent.ext_props);

        return dailyStaging;
    }

    public static EventsHourlyStaging commonEventsToHourlyStagingMapper(IECommonEvent commonEvent,
                                                                        String timeZone,
                                                                        String yyyymmdd,
                                                                        String hour) {
        EventsHourlyStaging hourlyStaging = new EventsHourlyStaging();

        hourlyStaging.setEvent_bucket(timeZone);
        hourlyStaging.setYyyymmdd(yyyymmdd);
        hourlyStaging.setHour(Integer.parseInt(hour));
        hourlyStaging.setResrc_type(commonEvent.resourceType);
        hourlyStaging.setResrc_uid(commonEvent.resourceId);
        hourlyStaging.setLog_uuid(UUIDs.timeBased());

        try {
            hourlyStaging.setEvent_ts(com.ge.current.em.analytics.common.DateUtils.getDateFromTimeStringTimeZone(yyyymmdd + hour, "yyyyMMddHH", timeZone));
        }catch (Exception e) {
            LOGGER.error("Failed to populate the event ts.", e);
        }

        hourlyStaging.setEnterprise_uid(commonEvent.enterpriseId);
        hourlyStaging.setRegion_name(commonEvent.regionName);
        hourlyStaging.setSite_city(commonEvent.siteCity);
        hourlyStaging.setSite_state(commonEvent.siteState);
        hourlyStaging.setSite_country(commonEvent.siteCountry);
        hourlyStaging.setSite_name(commonEvent.siteName);
        hourlyStaging.setZones(commonEvent.zones);
        hourlyStaging.setSegments(commonEvent.segments);
        hourlyStaging.setLabels(commonEvent.labels);

        // Measures
        hourlyStaging.setMeasures_aggr(commonEvent.measures_aggr);
        hourlyStaging.setMeasures_avg(commonEvent.measures_avg);
        hourlyStaging.setMeasures_max(commonEvent.measures_max);
        hourlyStaging.setMeasures_min(commonEvent.measures_min);
        hourlyStaging.setMeasures_cnt(commonEvent.measures_cnt);

        // Tags
        hourlyStaging.setTags(commonEvent.tags);

        // Ext Props
        hourlyStaging.setExt_props(commonEvent.ext_props);

        return hourlyStaging;
    }

    public static EventsMonthlyLog eventsMonthlyStagingToEventsMonthlyLogMapper(EventsMonthlyStaging eventsMonthlyStaging,
                                                                                Map<String, String> segmentLoadTypeMapping) throws Exception {

        EventsMonthlyLog eventsMonthlyLog = new EventsMonthlyLog();

        BeanUtils.copyProperties(eventsMonthlyLog, eventsMonthlyStaging);

        String resourceType = eventsMonthlyStaging.getResrc_type();
        if(resourceType.equalsIgnoreCase("SEGMENT") &&
                segmentLoadTypeMapping != null &&
                segmentLoadTypeMapping.containsKey(eventsMonthlyStaging.getResrc_uid())) {
            eventsMonthlyLog.setEvent_bucket(eventsMonthlyStaging.getEvent_bucket() + "." + resourceType + "." + segmentLoadTypeMapping.get(eventsMonthlyStaging.getResrc_uid()) + "." + eventsMonthlyStaging.getResrc_uid() + ".reading");
        } else {
            eventsMonthlyLog.setEvent_bucket(eventsMonthlyStaging.getEvent_bucket() + "." + resourceType + "." + eventsMonthlyStaging.getResrc_uid() + ".reading");
        }

        return eventsMonthlyLog;

    }

    public static EventsDailyLog eventsDailyStagingToEventsDailyLogMapper(EventsDailyStaging eventsDailyStaging,
                                                                          Map<String, String> segmentLoadTypeMapping) throws Exception {
        EventsDailyLog eventsDailyLog = new EventsDailyLog();

        BeanUtils.copyProperties(eventsDailyLog, eventsDailyStaging);

        String resourceType = eventsDailyStaging.getResrc_type();
        if(resourceType.equalsIgnoreCase("SEGMENT") &&
           segmentLoadTypeMapping != null &&
           segmentLoadTypeMapping.containsKey(eventsDailyStaging.getResrc_uid())) {
            eventsDailyLog.setEvent_bucket(eventsDailyStaging.getEvent_bucket() + "." + resourceType + "." + segmentLoadTypeMapping.get(eventsDailyStaging.getResrc_uid()) + "." + eventsDailyStaging.getResrc_uid() + ".reading");
        } else {
            eventsDailyLog.setEvent_bucket(eventsDailyStaging.getEvent_bucket() + "." + resourceType + "." + eventsDailyStaging.getResrc_uid() + ".reading");
        }

        return eventsDailyLog;
    }

    public static EventsHourlyLog eventsHourlyStagingToEventsHourlyMapper(EventsHourlyStaging eventsHourlyStaging,
                                                                          Map<String, String> segmentLoadTypeMapping) throws Exception {
        EventsHourlyLog eventsHourlyLog = new EventsHourlyLog();

        BeanUtils.copyProperties(eventsHourlyLog, eventsHourlyStaging);

        String resourceType = eventsHourlyStaging.getResrc_type();
        if(resourceType.equalsIgnoreCase("SEGMENT") &&
           segmentLoadTypeMapping != null &&
           segmentLoadTypeMapping.containsKey(eventsHourlyStaging.getResrc_uid())) {
            String eventBucket = eventsHourlyStaging.getEvent_bucket() + "." + resourceType + "." + segmentLoadTypeMapping.get(eventsHourlyStaging.getResrc_uid()).split(":")[0]  + "." + eventsHourlyStaging.getResrc_uid() + ".reading";
            System.out.println("Segment Type event bucket: " + eventBucket);
            eventsHourlyLog.setEvent_bucket(eventBucket);
        } else {
            eventsHourlyLog.setEvent_bucket(eventsHourlyStaging.getEvent_bucket() + "." + resourceType + "." + eventsHourlyStaging.getResrc_uid() + ".reading");
        }

        return eventsHourlyLog;
    }

    public static Function<Row, IECommonEvent> byMinuteStagingToCommonEventMapper = new Function<Row, IECommonEvent>() {
        @Override
        public IECommonEvent call(Row row) throws Exception {
            IECommonEvent commonEvent = new IECommonEvent();

            commonEvent.resourceId = row.getString(AggregationSQL.FieldIndex.BYMINUTE_STG_RESRC_UID_IDX);
            commonEvent.resourceType = row.getString(AggregationSQL.FieldIndex.BYMINUTE_STG_RESRC_TYPE_IDX);
            commonEvent.enterpriseId = row.getString(AggregationSQL.FieldIndex.BYMINUTE_STG_ENT_UID_IDX);
            commonEvent.timeZone = row.getString(AggregationSQL.FieldIndex.BYMINUTE_STG_TZ_IDX);

            // @formatter: off
            commonEvent.measures_aggr = row.isNullAt(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_AGGR_IDX)?
                                        new HashMap<>() :
                                        new HashMap<>(row.getJavaMap(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_AGGR_IDX));

            commonEvent.measures_cnt = row.isNullAt(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_CNT_IDX)?
                                       new HashMap<>() :
                                       new HashMap<>(row.getJavaMap(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_CNT_IDX));

            commonEvent.measures_min = row.isNullAt(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_MIN_IDX)?
                                       new HashMap<>() :
                                       new HashMap<>(row.getJavaMap(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_MIN_IDX));

            commonEvent.measures_max = row.isNullAt(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_MAX_IDX)?
                                       new HashMap<>() :
                                       new HashMap<>(row.getJavaMap(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_MAX_IDX));

            commonEvent.measures_avg = row.isNullAt(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_AVG_IDX)?
                                       new HashMap<>() :
                                       new HashMap<>(row.getJavaMap(AggregationSQL.FieldIndex.BYMINUTE_STG_MEASURES_AVG_IDX));

            commonEvent.tags = row.isNullAt(AggregationSQL.FieldIndex.BYMINUTE_STG_TAGS_IDX)?
                               new HashMap<>() :
                               new HashMap<>(row.getJavaMap(AggregationSQL.FieldIndex.BYMINUTE_STG_TAGS_IDX));

            commonEvent.ext_props = row.isNullAt(AggregationSQL.FieldIndex.BYMINUTE_STG_EXT_PROPS_IDX)?
                                    new HashMap<>() :
                                    new HashMap<>(row.getJavaMap(AggregationSQL.FieldIndex.BYMINUTE_STG_EXT_PROPS_IDX));

            // @formatter: on

            return commonEvent;
        }
    };

    public static Function2<IECommonEvent, IECommonEvent, IECommonEvent> aggregateCommonEvents = new Function2<IECommonEvent, IECommonEvent, IECommonEvent>() {
        @Override
        public IECommonEvent call(IECommonEvent r1, IECommonEvent r2) {
            try {
                r1.measures_min = ArithmeticUtil.getMinReduceFunc().call(r1.measures_min, r2.measures_min);
                r1.measures_max = ArithmeticUtil.getMaxReduceFunc().call(r1.measures_max, r2.measures_max);
                r1.measures_aggr = ArithmeticUtil.getSumReduceFunc().call(r1.measures_aggr, r2.measures_aggr);
                r1.measures_cnt = ArithmeticUtil.getSumReduceFunc().call(r1.measures_cnt, r2.measures_cnt);

                Map<String, Double> averageMap = new HashMap<>();
                for (String key : r1.measures_aggr.keySet()) {
                    Double aggr = r1.measures_aggr.get(key);
                    Double count = r1.measures_cnt.get(key.replace("measures_aggr", "measures_cnt"));
                    if (aggr == null ||  count == null) continue; 
                    averageMap.put(key.replace("measures_aggr", "measures_avg"), aggr / count);
                }
                r1.measures_avg = averageMap;

                // Merge all Tags
                r1.tags.putAll(r2.tags);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return r1;
        };
    };

    public static Function2<IECommonEvent, IECommonEvent, IECommonEvent> aggregateNormalizedEvents = new Function2<IECommonEvent, IECommonEvent, IECommonEvent>() {
        @Override
        public IECommonEvent call(IECommonEvent r1, IECommonEvent r2) {

            try {
                r1.measures_min = ArithmeticUtil.getMinReduceFunc().call(r1.measures_min, r2.measures_min);
                r1.measures_max = ArithmeticUtil.getMaxReduceFunc().call(r1.measures_max, r2.measures_max);
                r1.measures_aggr = ArithmeticUtil.getSumReduceFunc().call(r1.measures_aggr, r2.measures_aggr);
                r1.measures_cnt = ArithmeticUtil.getSumReduceFunc().call(r1.measures_cnt, r2.measures_cnt);
                
                Map<String, Double> averageMap = new HashMap<>();
                for (String key : r1.measures_aggr.keySet()) {
                    Double aggr = r1.measures_aggr.get(key);
                    Double count = r1.measures_cnt.get(key);
                    if (count != null) {
                        averageMap.put(key, aggr / count);
                    }
                }

                r1.measures_avg = averageMap;

                if (r2.tags != null) {
                    if (r1.tags == null) {
                        r1.tags = new HashMap<>();
                    }

                    // if r2 is more recent than r1
                    // update the return tags to have r2 values.
                    if (r2.eventTs.compareTo(r1.eventTs) > 0)  {
                        r2.tags.keySet().stream().forEach( key -> {
                            // remove the latest runtime for the key from r1
                            if (! key.contains("_runtime") && r1.tags.containsKey(key)) {
                                String tagValue = r1.tags.get(key);
                                String r1RuntimeTag = key + "_" + tagValue + "_runtime";
                                if (r1.tags.containsKey(r1RuntimeTag)) {
                                    r1.tags.remove(r1RuntimeTag);
                                }
                            }
                            // get the tag latest value and latest runtime from r2
                            // since r2 is more recent
                            r1.tags.put(key, r2.tags.get(key));
                        });

                        // update r1 timestamp to be latest.  
                        r1.eventTs = r2.eventTs;
                        System.out.println("*** Update r1 eventTs = " + r1.eventTs);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return r1;
        };
    };
}
