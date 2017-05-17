package com.ge.current.em.aggregation.mappers;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.aggregation.utils.MappingUtils;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.em.analytics.dto.JaceEvent;
import com.ge.current.ie.model.nosql.EventLog;
import com.ge.current.ie.model.nosql.NormalizedEventLog;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.time.ZoneId;

/**
 * Created by 212582112 on 2/16/17.
 */
public class EventsMapper implements Serializable{
    private static final long serialVersionUID = 1L;

    public EventLog generateEventLog(JaceEvent jaceEvent,
                                     Map<String, String> assetEnterpriseMapping,
                                     Map<String, String> assetSiteMapping) {
        if(jaceEvent == null) return null;

        EventLog eventLog = new EventLog();
        MappingUtils mappingUtils = new MappingUtils();

        eventLog.setTimeBucket(DateUtils.getTimeBucket(jaceEvent.eventTs));
        eventLog.setEnterpriseUid(mappingUtils.getResourceMappingUid(assetEnterpriseMapping, jaceEvent.assetUid, "UNKNOWN_ENTERPRISE"));
        eventLog.setResrc_uid(jaceEvent.assetUid);
        eventLog.setSiteUid(mappingUtils.getResourceMappingUid(assetSiteMapping, jaceEvent.assetUid, null));
        eventLog.setEventType(jaceEvent.eventType);
        eventLog.setEventTs(new Date(jaceEvent.eventTs)); // UTC event timestamp
        eventLog.setEventTsTz(jaceEvent.zonedDateTime.getZone().getId()); // Event timestamp timezone
        eventLog.setLogUuid(UUIDs.timeBased());
        eventLog.setMeasures(getPrefixMap(jaceEvent.rawMeasures, "measures"));
        eventLog.setTags(getPrefixTagMap(jaceEvent.rawTags, "tags"));

        return eventLog;
    }


    public NormalizedEventLog normalizeEvent(JaceEvent jaceEvent,
                                             Map<String, String> assetEnterpriseMapping,
                                             Map<String, String> assetSiteMapping,
                                             Map<String, String> assetTypeMapping,
                                             Map<String, String> assetTimezoneMapping,
                                             Map<String, Set<String>> assetSegmentsMapping,
                                             Map<String, String> segmentLoadtypeMapping,
                                             Map<String, String> resourceNameMapping) {

        if(jaceEvent == null) return null;

        NormalizedEventLog eventLog = new NormalizedEventLog();
        MappingUtils mappingUtils = new MappingUtils();

        String siteUid = mappingUtils.getResourceMappingUid(assetSiteMapping, jaceEvent.assetUid, null);
        eventLog.setTime_bucket(DateUtils.getTimeBucket(jaceEvent.eventTs));
        eventLog.setResrc_uid(jaceEvent.assetUid);
        eventLog.setEvent_type(jaceEvent.eventType);
        eventLog.setEvent_ts(new Date(jaceEvent.eventTs)); // UTC event timestamp
        
        // Decide the timezone of the assets/events.
        String jaceTimezone = jaceEvent.zonedDateTime.getZone().getId(); 
        String apmTimezone = mappingUtils.getResourceMappingUid(assetTimezoneMapping, 
               jaceEvent.assetUid, null); 

        eventLog.setEvent_ts_tz(jaceTimezone);
        //setEventTimezone(eventLog, jaceTimezone, apmTimezone);

        eventLog.setLog_uuid(UUIDs.timeBased());
        eventLog.setEnterprise_uid(mappingUtils.getResourceMappingUid(assetEnterpriseMapping, jaceEvent.assetUid, "UNKNOWN_ENTERPRISE"));
        eventLog.setSite_uid(siteUid);
        eventLog.setAsset_type(mappingUtils.getResourceMappingUid(assetTypeMapping, jaceEvent.assetUid, null));
        eventLog.setMeasures(getPrefixMap(jaceEvent.measures, "measures"));
        eventLog.setTags(getPrefixTagMap(jaceEvent.tags, "tags"));

        // Populate ext_props.
        Set<String> segmentsList = mappingUtils.getResourceMappingUid(assetSegmentsMapping, jaceEvent.assetUid, new HashSet<>());
        eventLog.setSegment_uid(StringUtils.join(segmentsList, ","));

        Map<String, String> ext_props = new HashMap<>();
        ext_props.put("site_uid", siteUid);
        ext_props.put("segment_uid", StringUtils.join(segmentsList, ","));
        ext_props.put("asset_type", mappingUtils.getResourceMappingUid(assetTypeMapping, jaceEvent.assetUid, null));
        ext_props.put("asset_name", mappingUtils.getResourceMappingUid(resourceNameMapping, jaceEvent.assetUid, "NO_NAME"));
        ext_props.put("site_name", mappingUtils.getResourceMappingUid(resourceNameMapping, siteUid, "NO_NAME"));


        String segmentLoadtype = mappingUtils.getAssetLoadName(segmentsList, segmentLoadtypeMapping);
        if (segmentLoadtype != null) {
            ext_props.put("LT", segmentLoadtype);
        }
        eventLog.setExt_props(ext_props);
        System.out.println("*** Saving to norm_log table:  asset : " + jaceEvent.assetUid 
                + " Event ts: " + jaceEvent.eventTs + "  time_bucket = "   
                + DateUtils.getTimeBucket(jaceEvent.eventTs));


        return eventLog;
    }

    private void setEventTimezone(NormalizedEventLog eventLog, String jaceTimezone,
            String apmTimezone) 
    {
        eventLog.setEvent_ts_tz(jaceTimezone);

        String assetUid = eventLog.getResrc_uid();
        try {
            ZoneId zid = ZoneId.of(apmTimezone);
            eventLog.setEvent_ts_tz(apmTimezone);
            System.out.println("SET ASSET Timezone: " + assetUid 
                    + " using APM timezone: " + apmTimezone);
        } catch (Exception e) {
            System.out.println("ASSET: " + assetUid 
                    + " timezone is not configured correctly !!"
                    + " configured timezone value = " + apmTimezone);
        }

        // another way to get the timezone IDs
            /*
            if (apmTimezone != null && 
                    ZoneId.SHORT_IDS.containsKey(apmTimezone.toUpperCase()))  {
                String timezone = ZoneId.SHORT_IDS.get(apmTimezone);

                // let's use apm timezone only if it's configured correctly. 
                if(timezone.equals("-05:00")) {
                    timezone = "America/New_York";
                }
                eventLog.setEvent_ts_tz(timezone);
                System.out.println("SET ASSET Timezone: " + jaceEvent.assetUid 
                    + " using APM timezone: " + apmTimezone);
             }
             */
    } 

    private Map<String, Double>  getPrefixMap(Map<String, Double> inputMap, String prefix) {
        Map<String, Double> newMap = new HashMap(); 
        for(String key: inputMap.keySet()) {
            newMap.put(prefix+key, inputMap.get(key));
        }
        return newMap;
    }

    private Map<String, String>  getPrefixTagMap(Map<String, String> inputMap, String prefix) {
        Map<String, String> newMap = new HashMap(); 
        for(String key: inputMap.keySet()) {
            newMap.put(prefix+key, inputMap.get(key));
        }
        return newMap;
    }
}
