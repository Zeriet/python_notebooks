package com.ge.current.em.util;

import com.ge.current.em.entities.analytics.AlarmObject;
import com.ge.current.em.entities.analytics.Event;
import com.ge.current.em.entities.analytics.RulesBaseFact;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ge.current.em.util.QueryUtil.ENTERPRISE_UID_INDEX;
import static com.ge.current.em.util.QueryUtil.EVENT_TS_INDEX;
import static com.ge.current.em.util.QueryUtil.EVENT_TS_TZ_INDEX;
import static com.ge.current.em.util.QueryUtil.EXT_PROPS_INDEX;
import static com.ge.current.em.util.QueryUtil.HHMM_INDEX;
import static com.ge.current.em.util.QueryUtil.MEASURES_AGGR_INDEX;
import static com.ge.current.em.util.QueryUtil.MEASURES_AVG_INDEX;
import static com.ge.current.em.util.QueryUtil.RESC_TYPE_INDEX;
import static com.ge.current.em.util.QueryUtil.RESRC_UID_INDEX;
import static com.ge.current.em.util.QueryUtil.TAGS_INDEX;
import static com.ge.current.em.util.QueryUtil.YYMMDD_INDEX;

public class RddUtil {

    static final Logger LOG = LoggerFactory.getLogger(RddUtil.class);

	public static Event getEventFromTableRow(Row cassandraRow, String tableName){
		Event event = null;
		if("events_byminute_stg".equals(tableName)) {
			event = getEventFromTableRowByMin( cassandraRow);
		} else if("event_norm_log".equals(tableName)) {
			event = getEventFromNormLog(cassandraRow);
		}
		return event;
	}


	public static Event getEventFromTableRowByMin(Row cassandraRow) {
		Event event = new Event();
		event.setAssetId(cassandraRow.getString(RESRC_UID_INDEX));
		event.setAssetType(cassandraRow.getString(RESC_TYPE_INDEX));
		event.setEnterpriseId(cassandraRow.getString(ENTERPRISE_UID_INDEX));
		event.setHhmm(cassandraRow.getString(HHMM_INDEX));
		Map<Object, Object> measuresAvg = cassandraRow.getJavaMap(MEASURES_AVG_INDEX);
		Map<String, Object> map = new HashMap<String, Object>((Map) measuresAvg);
		event.setMeasuresAvg((Map<String, Object>) map);
		Map<Object, Object> measuresAggr = cassandraRow.getJavaMap(MEASURES_AGGR_INDEX);
		map = new HashMap<String, Object>((Map) measuresAggr);
		event.setMeasuresAggr((Map<String, Object>) map);

		event.setYymmdd(cassandraRow.getString(YYMMDD_INDEX));
		event.setHhmm(cassandraRow.getString(HHMM_INDEX));


		Map<Object, Object> tags = cassandraRow.getJavaMap(TAGS_INDEX);

		if(tags != null && !tags.isEmpty()) {
			Map<String, String> tagsMap = new HashMap<String, String>((Map) tags);
			event.setTags(tagsMap);
		}
		Map<Object, Object> extProps = cassandraRow.getJavaMap(EXT_PROPS_INDEX);
		if(extProps != null) {
			Map<String, String> extPropsMap = new HashMap<String, String>((Map) extProps);
			event.setSiteId(extPropsMap.get(Event.EXT_PROP_SITE_UID));
			event.setExtProps(extPropsMap);

			LOG.info("setting events site_uid: {}",extPropsMap.get(Event.EXT_PROP_SITE_UID));
		} else {
			event.setExtProps(new HashMap<>());
		}

		return event;
	}

    public static Event getEventFromNormLog(Row cassandraRow) {
        Event event = new Event();
        event.setAssetId(cassandraRow.getString(RESRC_UID_INDEX));
        event.setAssetType(cassandraRow.getString(RESC_TYPE_INDEX));
        event.setEnterpriseId(cassandraRow.getString(ENTERPRISE_UID_INDEX));
        String eventTS = cassandraRow.get(EVENT_TS_INDEX).toString();
        String eventTsTz = cassandraRow.getString(EVENT_TS_TZ_INDEX);

        LOG.info(" ****** evenTSTZ *************: {}", eventTsTz);
        String yyyyMMdHHmm = null;
        try {
             yyyyMMdHHmm = EmUtil.convertEventTsToLocalDateTime(eventTS, eventTsTz);
        } catch (ParseException e) {
            LOG.info(" Exception on parsing event_ts");
        }
    	event.setHhmm(yyyyMMdHHmm.substring(8,12));
        event.setYymmdd(yyyyMMdHHmm.substring(0,8));
        Map<Object, Object> measures = cassandraRow.getJavaMap(MEASURES_AVG_INDEX);
        Map<String, Object> map = new HashMap<String, Object>((Map) measures);
        event.setMeasures(map);
        Map<Object, Object> tags = cassandraRow.getJavaMap(TAGS_INDEX);
        Map<String, String> tagsMap = new HashMap<String, String>((Map) tags);
        event.setTags(tagsMap);
        Map<Object, Object> extProps = cassandraRow.getJavaMap(EXT_PROPS_INDEX);
        Map<String, String> extPropsMap = new HashMap<String, String>((Map) extProps);
        event.setSiteId(extPropsMap.get("siteId"));
        event.setExtProps(extPropsMap);
        LOG.info("************ event: {}", event);
        return event;
    }

	public static RulesBaseFact getRulesBaseFact(Iterable<Event> events, List<String> requiredMeasuresAvgPoints, List<String> requiredMeasuresAggrPoints, List<String> requiredTagsPoints, Map<String, Object> parametersMap) {
		RulesBaseFact ruleBaseFact = new RulesBaseFact();
		List<Map<String, Object>> factMeasuresAvg = new ArrayList<>();
		List<Map<String, Object>> factMeasuresAggr = new ArrayList<>();
		List<Map<String, Object>> factTags = new ArrayList<>();
		Map<String, Object> measuresMap;

		for (Event event : events) {
			initializeRulesBaseFactFromEvents(event, ruleBaseFact,parametersMap);
			LOG.info("ruleBaseFact: key: {}, time: {}", event.getYymmdd()+ event.getHhmm());

			measuresMap = getMapWithRequiredPoints(requiredMeasuresAvgPoints, event.getMeasuresAvg(), event);
			factMeasuresAvg.add(measuresMap);
			ruleBaseFact.setMeasuresAvgMap(factMeasuresAvg);

			measuresMap = getMapWithRequiredPoints(requiredMeasuresAggrPoints,event.getMeasuresAggr(), event);
			factMeasuresAggr.add(measuresMap);
			ruleBaseFact.setMeasuresAggrMap(factMeasuresAggr);

			Map<String, Object> tagsMap = getMapWithRequiredPoints(requiredTagsPoints,event.getTags(), event);
			factTags.add(tagsMap);
			ruleBaseFact.setExt_properties(event.getExtProps());
		}

		ruleBaseFact.setMeasuresAvgMap(factMeasuresAvg);
		ruleBaseFact.setMeasuresAggrMap(factMeasuresAggr);
		ruleBaseFact.setTagsMap(factTags);
		return ruleBaseFact;
	}

	private static Map<String, Object> getMapWithRequiredPoints(List<String> requiredPointNames,Map<String, ?> eventsMap,  Event event) {
		Map<String, Object> mapWithRequiredPoints = new HashMap<>();
		for (String requiredPointsString : requiredPointNames) {
			String [] requiredPoints = requiredPointsString.split(",");
			for(String requiredPoint : requiredPoints) {
				if(eventsMap.containsKey(requiredPoint)) {
					mapWithRequiredPoints.put(requiredPoint,eventsMap.get(requiredPoint));
					LOG.info("tagsKey: {} TagsMap: {}", requiredPoint,eventsMap.get(requiredPoint));
				}
			}

			mapWithRequiredPoints.put("event_ts", event.getYymmdd() + event.getHhmm());
			mapWithRequiredPoints.put("asset_id", event.getAssetId());

		}
		return mapWithRequiredPoints;
	}

	public static void initializeRulesBaseFactFromEvents(Event event, RulesBaseFact rulesBaseFact, Map<String,Object> parametersMap) {
		rulesBaseFact.setAssetId(event.getAssetId());
		rulesBaseFact.setConditionMet(false);
		rulesBaseFact.setParameters(parametersMap);
		LOG.info("Site ID while initializing rulesBaseFact: {}, enterpriseId : {} ",event.getSiteId(), event.getEnterpriseId());
		parametersMap.forEach((k,v)-> LOG.info(("*********key value: " + k+", "+v)));
		rulesBaseFact.setAlarmObject(new AlarmObject());
		rulesBaseFact.setTagsMap(new ArrayList<>());
		rulesBaseFact.setEnterpriseId(event.getEnterpriseId());
		rulesBaseFact.setSegmentId(event.getSegmentId());
		rulesBaseFact.setSiteId(event.getSiteId());
		rulesBaseFact.setHhmm(event.getHhmm());
		rulesBaseFact.setAssetType(event.getAssetType());
	}

}
