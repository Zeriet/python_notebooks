package com.ge.current.em.custom.analytics.batch;

import com.ge.current.em.entities.analytics.Event;
import com.ge.current.em.entities.analytics.RulesBaseFact;
import com.ge.current.em.util.EmUtil;
import com.ge.current.em.util.QueryUtil;
import com.ge.current.em.util.RddUtil;
import com.ge.current.em.util.RulesUtil;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.ge.current.em.util.QueryUtil.EXT_PROPS_INDEX;
import static com.ge.current.em.util.QueryUtil.HHMM_INDEX;
import static com.ge.current.em.util.QueryUtil.RESC_TYPE_INDEX;
import static com.ge.current.em.util.QueryUtil.RESRC_UID_INDEX;
import static com.ge.current.em.util.QueryUtil.TAGS_INDEX;
import static com.ge.current.em.util.QueryUtil.YYMMDD_INDEX;
import static com.ge.current.em.util.QueryUtil.getQueryStringForTable;

@Component @PropertySource(value = "classpath:/application-${spring.profiles.active}.properties")
public class BasicISparkRuleImpl implements ISparkRule, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BasicISparkRuleImpl.class);
	private static final String TIMEBUCKET_DATE_FORMAT = "yyyyMMddHHmm";

	@Autowired
	public PropertiesBean propertiesBean;

	@Value("${spring.profiles.active}")
	private String activeProfile;
	private String date;
	private String rescrType;
	private String endTime;
	private String tableName;
	private String duration;

	private String timeFrame;
	private String groupBy;
	private String ruleNamesAsString;

	private String appName;
	private String timeZone = null;
	private List<String> ruleNames = null;
	private CassandraSQLContext cassandraSQLContext = null;
	transient private SparkContext sc = null;

	public Broadcast<Map<String, String>> rulesUidDrlMappingBroadcast = null;
	public Broadcast<Map<String, Map<String, Object>>> parameterMapBroadcast = null;
	public Broadcast<Map<String, String>> ruleRegistryMapBroadcast = null;
	public Broadcast<Map<String, List<String>>> ruleRequiredMeasuresMapBroadcast = null;
	public Broadcast<Map<String, List<String>>> ruleRequiredMeasuresAvgMapBroadcast = null;
	public Broadcast<Map<String, List<String>>> ruleRequiredMeasuresAggrMapBroadcast = null;
	public Broadcast<Map<String, List<String>>> ruleRequiredTagsMapBroadcast = null;
	public Broadcast<Map<String, List<String>>> ruleRequiredAssetTypeMapBroadcast = null;
	// contains a mapping of rule_uid:rule_name
	public Broadcast<Map<String, String>> ruleNamesMappingBroadcast = null;

	@Override public String getQuery(PropertiesBean propertiesBean) {
		LOG.info("yyyymmdd: {} ", getDate());
		String cql = "";

		try {
			cql =  getQueryStringForTable(propertiesBean.getValue(SparkConfigurations.CASSANDRA_KEYSPACE), getTableName(), getTimeZone(), getTimeBucket(getDate(), getEndTime()), getDate(), getEndTime());
		} catch (ParseException e) {
			LOG.error("Error parsing cql: {}", e);
		}

		LOG.info("CQL: " + cql);
		return cql;
	}

	@Override public JavaRDD<Row> getFilteredInput(JavaRDD<Row> df) {

		LOG.warn("Rows before filtering: " + df.count());
		JavaRDD<Row> rowsRDD = df.filter(new Function<Row, Boolean>() {

			@Override public Boolean call(Row inputRow) throws Exception {
				if(inputRow == null) {
					LOG.info("Input row is null. Skipping filtering...");
					return false;
				}
				LOG.info("***************************************************************************");
				LOG.info("Analyzing record for resource: {}", inputRow.get(RESRC_UID_INDEX));
//				LOG.info("hhmm: {}" , inputRow.getString(HHMM_INDEX) );
				LOG.info("Filtering based on time frame...");
				if (!filterTimeFrame(inputRow)) {
					LOG.info("Filtering based on time frame: Failed.");
					LOG.info("***************************************************************************");
					return false;
				} else {
					LOG.info("Filtering based on time frame: Passed.");
				}

				if (!filterTimeZone(inputRow)) {
					LOG.info("Filtering based on time Zone: Failed.");
					LOG.info("***************************************************************************");
					return false;
				} else {
					LOG.info("Filtering based on time Zone: Passed.");
				}

				LOG.info("Filtering based on resource type...");
				if (!filterResrcType(inputRow)) {
					LOG.info("Filtering based on resource type: Failed.");
					LOG.info("***************************************************************************");
					return false;
				} else {
					LOG.info("Filtering based on resource type: Passed.");
				}

				LOG.info("Filtering records that have null resource type...");
				if(!filterNullResrc(inputRow)) {
					LOG.info("Filtering records that have null resource type: Failed.");
					LOG.info("***************************************************************************");
					return false;
				} else {
					LOG.info("Filtering records that have null resource type: Passed.");
				}

				LOG.info("Filtering based on asset type...");
				if (!filterAssetTypes(inputRow)) {
					LOG.info("Filtering based on asset type: Failed");
					LOG.info("***************************************************************************");
					return false;
				} else {
					LOG.info("Filtering based on asset type: Passed.");
				}

				LOG.info("Filtering based on points...");
				if (!filterPoints(inputRow)) {
					LOG.info("Filtering based on points: Failed");
					LOG.info("***************************************************************************");
					return false;
				} else {
					LOG.info("Filtering based on points: Passed.");
				}
				LOG.info("Current record passed.");
				LOG.info("***************************************************************************");
				return true;
			}
		});
		LOG.warn("Rows after filtering: " + rowsRDD.count());
		return rowsRDD;
	}

	private boolean filterTimeFrame(Row inputRow) throws ParseException {
		if(QueryUtil.NORM_LOG_TABLENAME.equals(getTableName())){
			return true;
		}

		String yyyymmdd = inputRow.getString(YYMMDD_INDEX);
		String hhmm = inputRow.getString(HHMM_INDEX);
		SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmm");
		sf.setTimeZone(TimeZone.getTimeZone(getTimeZone()));
		Date inputputRowDate = sf.parse(yyyymmdd + hhmm);
		LOG.info("String before inputputRowDate formatting, {}", yyyymmdd + hhmm);
		LOG.info("filterTimeFrame: inputputRowDate {}", inputputRowDate);
		Date startDateTime = sf.parse(getDate());
		LOG.info("String before startDateTime formatting, {}", getDate());
		LOG.info("filterTimeFrame: startDateTime {}", startDateTime);
		Date endDateTime = sf.parse(getEndTime());
		LOG.info("String before endDateTime formatting, {}", getEndTime());
		LOG.info("filterTimeFrame: endDateTime {}", endDateTime);

		if((inputputRowDate.after(startDateTime) || inputputRowDate.equals(startDateTime)) && inputputRowDate.before(endDateTime)) {
			return true;
		}

		return false;
	}

	private boolean filterTimeZone(Row inputRow) {
		if(QueryUtil.NORM_LOG_TABLENAME.equals(getTableName())) {
			if (inputRow.getString(QueryUtil.EVENT_TS_TZ_INDEX) != null) {
				if (this.getTimeZone().equalsIgnoreCase(inputRow.getString(QueryUtil.EVENT_TS_TZ_INDEX))) {
					return true;
				} else {
					return false;
				}
			}
		}
		return true;
	}

	private boolean filterResrcType(Row inputRow) {
		if(QueryUtil.NORM_LOG_TABLENAME.equals(getTableName())) {
			return true;
		}
		if (inputRow.getString(RESC_TYPE_INDEX) == null) {
			return false;
		}
		if (!"ASSET".equalsIgnoreCase(inputRow.getString(RESC_TYPE_INDEX ))) {
			return false;
		}
		return true;
	}

	private boolean filterNullResrc(Row inputRow) {
		LOG.info("resrc_uid: {}, resourceType: {}, groupbyKey: {} ", inputRow.getString(RESRC_UID_INDEX ), inputRow.getString(RESC_TYPE_INDEX ), getGroupBy());
		if ("site".equalsIgnoreCase(getGroupBy())) {
			Map<Object, Object> extProps = inputRow.getJavaMap(EXT_PROPS_INDEX);
			if(extProps != null && extProps.get("site_uid") != null) {
				return true;
			}
		}
		if ("ASSET".equalsIgnoreCase(getGroupBy()) && inputRow.getString(RESRC_UID_INDEX ) != null) {
			return true;
		}
		return false;
	}

	private boolean filterAssetTypes(Row inputRow) {
		Map<String, String> ext_props = inputRow.getJavaMap(QueryUtil.EXT_PROPS_INDEX);
		LOG.info("External properties: {}", ext_props);
		if (ext_props != null && ext_props.get("asset_type") != null) {
			if (containsRequiredAssetTypes(ext_props.get("asset_type"))) {
				return true;
			}
		}
		return false;
	}

	private boolean filterPoints(Row inputRow) {

		if(!QueryUtil.NORM_LOG_TABLENAME.equals(getTableName())){
			Map<String, Double> measuresAvgMap = inputRow.getJavaMap(QueryUtil.MEASURES_AVG_INDEX);

			Map<String, Double> measuresAggrMap = inputRow.getJavaMap(QueryUtil.MEASURES_AGGR_INDEX);

			if (!containsRequiredMeasuresAggr(measuresAggrMap)) {
				return false;
			}
			if(!containsRequiredMeasuresAvg(measuresAvgMap)) {
				return false;
			}
		}

		Map<String, String> tagsMap = inputRow.getJavaMap(TAGS_INDEX);

		if(!containsRequiredTags(tagsMap)) {
			return false;
		}
		return true;
	}

	private boolean filterSiteTypes(Row inputRow) {
		return true;
	}

	private boolean filterEnterpriseTypes(Row inputRow) {
		return true;
	}

	@Override public List<String> getRulesStringList() {
		List<String> rulesList = new ArrayList<>();
		Map<String, String> rulesMap = rulesUidDrlMappingBroadcast.getValue();
		for (String ruleUid : ruleNames) {
			LOG.info("get the drl: rule_uid: {}", ruleUid);
			rulesList.add(rulesMap.get(ruleUid));
		}
		return rulesList;
	}

	@Override public JavaRDD<RulesBaseFact> generateRuleData(JavaRDD<Row> rowsRDD) {

		JavaRDD<Event> eventsRDD = rowsRDD.map(new Function<Row, Event>() {

			@Override public Event call(Row cassandraRow) throws Exception {
				return RddUtil.getEventFromTableRow(cassandraRow, getTableName());
			}
		});

		JavaPairRDD<String, Event> eventsByAssets = eventsRDD.mapToPair(keyData).filter(keyData -> keyData != null);

		JavaPairRDD<String, Iterable<Event>> groupedEventsByAssetId = eventsByAssets.groupByKey();

		JavaRDD<RulesBaseFact> factRdd = groupedEventsByAssetId.map(new Function<Tuple2<String, Iterable<Event>>, RulesBaseFact>() {

			@Override public RulesBaseFact call(Tuple2<String, Iterable<Event>> events) throws Exception {
				LOG.info("pairedRDD: groupByKey: {}, elements: {}",events._1(), events._2.toString());
				RulesBaseFact ruleBaseFact = getRulesBaseFact(events._2(),getListOfRequiredMeasures(ruleNames),getListOfRequiredMeasuresAvg(ruleNames), getListOfRequiredMeasuresAggr(ruleNames),getListOfRequiredTags(ruleNames));
				return ruleBaseFact;
			}
		});
		return factRdd;
	}


	public RulesBaseFact getRulesBaseFact( Iterable<Event> events,List<String> requiredMeasuresPoints,List<String> requiredMeasuresAvgPoints, List<String> requiredMeasuresAggrPoints,List<String> requiredTagsPoints) {
		RulesBaseFact ruleBaseFact = new RulesBaseFact();
		List<Map<String, Object>> factMeasures = new ArrayList<>();
		List<Map<String, Object>> factMeasuresAvg = new ArrayList<>();
		List<Map<String, Object>> factMeasuresAggr = new ArrayList<>();
		List<Map<String, Object>> factTags = new ArrayList<>();
		Map<String, Object> measuresMap;

		for (Event event : events) {
			initializeRulesBaseFactFromEvents(event, ruleBaseFact);

			measuresMap = getMapWithRequiredPoints(requiredMeasuresAvgPoints, event.getMeasuresAvg(), "measures_avg", event);
			if(measuresMap != null && !measuresMap.isEmpty()) {
				factMeasuresAvg.add(measuresMap);
			}

			if(!requiredMeasuresPoints.isEmpty() && event.getMeasures() != null) {
				measuresMap = getMapWithRequiredPoints(requiredMeasuresPoints, event.getMeasures(), "measures", event);
				if(measuresMap != null && !measuresMap.isEmpty()){
					factMeasures.add(measuresMap);
				}
			}

			if(!requiredMeasuresAggrPoints.isEmpty() && event.getMeasuresAggr() != null) {
				measuresMap = getMapWithRequiredPoints(requiredMeasuresAggrPoints, event.getMeasuresAggr(), "measures_aggr", event);
				if(measuresMap != null && !measuresMap.isEmpty()){
					factMeasuresAggr.add(measuresMap);
				}
			}

			if(!requiredTagsPoints.isEmpty() &&  event.getTags() != null) {
				Map<String, Object> tagsMap = getMapWithRequiredPoints(requiredTagsPoints, event.getTags(), "tags", event);
				if(tagsMap != null && !tagsMap.isEmpty()){
					factTags.add(tagsMap);
				}
			}
			ruleBaseFact.setExt_properties(event.getExtProps());
		}
		if(factMeasuresAvg != null && !factMeasuresAvg.isEmpty()) {
			ruleBaseFact.setMeasuresAvgMap(EmUtil.sortListOfMapsBasedOnKey(factMeasuresAvg, "event_ts"));
		}
		if (factMeasures != null && !factMeasures.isEmpty()){
			ruleBaseFact.setMeasuresMap(EmUtil.sortListOfMapsBasedOnKey(factMeasures,"event_ts"));
		}
		if (factMeasuresAggr != null && !factMeasuresAggr.isEmpty()){
			ruleBaseFact.setMeasuresAggrMap(EmUtil.sortListOfMapsBasedOnKey(factMeasuresAggr,"event_ts"));
		}
		if (factTags != null && !factTags.isEmpty()){
			ruleBaseFact.setTagsMap(EmUtil.sortListOfMapsBasedOnKey(factTags,"event_ts"));
		}
		return ruleBaseFact;
	}

	private Map<String, Object> getMapWithRequiredPoints(List<String> requiredPointNames,Map<String, ?> eventsMap, String prefix, Event event) {
		Map<String, Object> mapWithRequiredPoints = new HashMap<>();
		for (String requiredPointsString : requiredPointNames) {
			String [] requiredPoints = requiredPointsString.split(",");
			for(String requiredPoint : requiredPoints) {
				String pointName = prefix + requiredPoint;
				if(eventsMap.containsKey(pointName) && eventsMap.get(pointName) != null) {
					mapWithRequiredPoints.put(requiredPoint,eventsMap.get(pointName));
					LOG.info("tagsKey: {} TagsMap: {}", requiredPoint,eventsMap.get(pointName));
				}
			}

			mapWithRequiredPoints.put("event_ts", event.getYymmdd() + event.getHhmm());
			mapWithRequiredPoints.put("asset_id", event.getAssetId());

		}
		return mapWithRequiredPoints;
	}

	public String getTimeBucket(String startDatetime, String endDateTime) throws ParseException {

		String endTime = getEndDate(endDateTime);

		String startTime = getStartDate(startDatetime);

		return (startTime.equals(endTime)) ? startTime : (startTime + "','" + endTime);
	}

	@Override
	public String getStartDate(String time) throws ParseException {
		LOG.info("Getting start date...");
		LOG.info("time frame: {}", timeFrame);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		sdf.setTimeZone(TimeZone.getTimeZone(getTimeZone()));
		LOG.info("timezone: {}", getTimeZone());
		Date date = sdf.parse(time);
		LOG.info("getTimeBucket: {}", date.toString());
		Calendar today = Calendar.getInstance();

		today.clear(Calendar.YEAR);
		today.clear(Calendar.MONTH);
		today.clear(Calendar.DAY_OF_MONTH);
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);

		today.setTimeZone(TimeZone.getTimeZone(getTimeZone()));
		today.setTime(date);

		int  month = today.get(Calendar.MONTH) + 1;
		int  day = today.get(Calendar.DAY_OF_MONTH);
		LOG.info("yyyymmdd: {}" , today.get(Calendar.YEAR) +
				(("" + month).length() < 2 ? "0" + month: month +"" )
				+ (("" + day).length() < 2 ? "0" + day : day + ""));

		String mm = ((""+ month).length() < 2? "0" + month: month +"" );
		String dd = ("" + day).length() < 2 ? "0" + day: day +"" ;
		String startTime = today.get(Calendar.YEAR) + mm +""+ dd;
		return startTime;

	}

	@Override
	public String getEndDate(String time) throws ParseException {
		LOG.info("Getting end date..");
		LOG.info("time frame: {}", timeFrame);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		sdf.setTimeZone(TimeZone.getTimeZone(getTimeZone()));
		LOG.info("timezone: {}", getTimeZone());
		Date date = sdf.parse(time);
		LOG.info("getTimeBucket: {}", date.toString());
		Calendar today = Calendar.getInstance();

		today.clear(Calendar.YEAR);
		today.clear(Calendar.MONTH);
		today.clear(Calendar.DAY_OF_MONTH);
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);

		today.setTimeZone(TimeZone.getTimeZone(getTimeZone()));
		today.setTime(date);


		int  month = today.get(Calendar.MONTH) + 1;
		int  day = today.get(Calendar.DAY_OF_MONTH);
		LOG.info("yyyymmdd: {}" , today.get(Calendar.YEAR) +
				(("" + month).length() < 2 ? "0" + month: month +"" )
				+ (("" + day).length() < 2 ? "0" + day : day + ""));

		String mm = ((""+ month).length() < 2? "0" + month: month +"" );
		String dd = ("" + day).length() < 2 ? "0" + day: day +"" ;
		String endTime = today.get(Calendar.YEAR) + mm +""+ dd;
		return endTime;
	}

	@Override
	public String getPreviousTimeBucket(String currentTimeBucket, String jobFrequencyInMinutes) throws ParseException {
		String previousTimeBucketInString;
		SimpleDateFormat timeBucketFormat = new SimpleDateFormat(TIMEBUCKET_DATE_FORMAT);
		Date timeBucket = timeBucketFormat.parse(currentTimeBucket);
		int jobFrequency = Integer.valueOf(jobFrequencyInMinutes);
		if (jobFrequency > 0) {
			jobFrequency *= -1;
		}
		Date previousTimeBucket = DateUtils.addMinutes(timeBucket, jobFrequency);
		previousTimeBucketInString = timeBucketFormat.format(previousTimeBucket);
		return previousTimeBucketInString;
	}

	private String getStartHourMin(String startTime) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		Date date = sdf.parse(startTime);
		Calendar today = Calendar.getInstance();
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);
		today.setTime(date);
		today.add(Calendar.MINUTE, Integer.valueOf("-" + getTimeFrame()));
		String hh = today.get(Calendar.HOUR_OF_DAY) < 10 ? "0" + today.get(Calendar.HOUR_OF_DAY) : today.get(Calendar.HOUR_OF_DAY) +"";
		String mm = today.get(Calendar.MINUTE) < 10 ? "0" + today.get(Calendar.MINUTE) : today.get(Calendar.MINUTE) +"";
		String hhmm = hh + mm;
		LOG.info("Endhhmm:******************** {}", hhmm);
		return hhmm;
	}

	private String getEndHourMin(String endTime) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHHmm");
		Date date = sdf.parse(endTime);
		Calendar today = Calendar.getInstance();
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);
		today.setTime(date);
		Date todayDate = today.getTime();
		String hh = today.get(Calendar.HOUR_OF_DAY) < 10 ? "0" + today.get(Calendar.HOUR_OF_DAY) : today.get(Calendar.HOUR_OF_DAY) +"";
		String mm = today.get(Calendar.MINUTE) < 10 ? "0" + today.get(Calendar.MINUTE) : today.get(Calendar.MINUTE) +"";
		String hhmm = hh + mm;
		LOG.info("Endhhmm:******************** {}", hhmm);
		return hhmm;
	}

	private PairFunction<Event, String, Event> keyData = new PairFunction<Event, String, Event>() {

		public Tuple2<String, Event> call(Event x) {
			LOG.info("group by key event: {}", x);
			if(x == null) {
				return null;
			}
			if("site".equalsIgnoreCase(getGroupBy())) {
				return new Tuple2(x.getSiteId(), x);
			}else {
				return new Tuple2(x.getAssetId(), x);
			}
		}
	};

	/**
	 * @return a map where keys=parameter_name + rule_uid, values=parameter_values
	 */
	public Map<String, Object> getParametersMap(String enterpriseId, String siteId) {
		Map<String, Map<String, Object>> parametersMap = parameterMapBroadcast.getValue();
		Map<String, String> ruleNamesMapping = ruleNamesMappingBroadcast.getValue();
		Map<String, Object> parameters = new HashMap<>();
		for (String ruleName : ruleNames) {
			if(ruleName != null) {
				String ruleId = getRuleIdFromRuleUid(enterpriseId, siteId, ruleName);
				if(ruleId != null) {
					LOG.info("BUILDING PARAMETERS ruleId: {}", ruleId);
					if (parametersMap.get(ruleId) != null) {
						for (String parameterName : parametersMap.get(ruleId).keySet()) {
							LOG.info("parameterName: {}", parameterName);
							parameters.put(parameterName + RulesUtil.getFDSINumber(ruleName),
									Double.valueOf(parametersMap.get(ruleId).get(parameterName).toString()));
							LOG.info("<parameter: values> --> <{} : {}>",  parameterName + RulesUtil.getFDSINumber(ruleName),  Double.valueOf(parametersMap.get(ruleId).get(parameterName).toString()));
						}
					}
				}
			}
		}
		LOG.info("BUILT PARAMETERS: {}", parameters);
		return parameters;
	}


	private String getRuleIdFromRuleUid(String enterpriseId, String siteId,String ruleUid) {
		Map<String, String> ruleUidToRuleIdMapping = ruleRegistryMapBroadcast.getValue();
		String id = ruleUid + "_" + siteId;
		LOG.info("id : " + id);
		LOG.info("ruleId from getRuleIdFromUid: {}", ruleUidToRuleIdMapping.get(ruleUid + "_" + siteId) != null ? ruleUidToRuleIdMapping.get(ruleUid + "_" + siteId) : ruleUidToRuleIdMapping.get(ruleUid + "_" + enterpriseId));
		return ruleUidToRuleIdMapping.get(ruleUid + "_" + siteId) != null ? ruleUidToRuleIdMapping.get(ruleUid + "_" + siteId) : ruleUidToRuleIdMapping.get(ruleUid + "_" + enterpriseId);
	}

	@Override public String getActiveProfile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override public void broadcast(SparkContext sc, PropertiesBean propertiesBean) {
		SQLContext sqlContext = EmUtil.initSqlContextAndCache(sc, propertiesBean);
		Map<String, String> rulesUidDrlMapping = EmUtil.getAllDocumentForRuleId(sqlContext, null);
		Map<String, Map<String, Object>> propertiesMap = EmUtil.getRulePropertiesMap(sqlContext, null);
		Map<String, List<String>> ruleRegistryMeasuresAvgMap = EmUtil.getRuleMeasuresAvgMap(sqlContext, null);
		Map<String, List<String>> ruleRegistryMeasuresAggrMap = EmUtil.getRuleMeasuresAggrMap(sqlContext, null);
		Map<String, List<String>> ruleRegistryMeasuresMap = EmUtil.getRuleMeasuresMap(sqlContext, null);
		Map<String, List<String>> ruleRegistryTagsMap = EmUtil.getRuleTagsMap(sqlContext, null);
		Map<String, String> ruleRegistryMap = EmUtil.getRuleRegistryMap(sqlContext, null);
		Map<String, String> ruleNamesMapping = EmUtil.getRuleNamesMapping(sqlContext);
		Map<String, List<String>> ruleRequiredAssetType = EmUtil.getRuleAssetTypeFilterMap(sqlContext, null);

		rulesUidDrlMappingBroadcast = sc.broadcast(rulesUidDrlMapping, scala.reflect.ClassTag$.MODULE$.apply(rulesUidDrlMapping.getClass()));
		parameterMapBroadcast = sc
				.broadcast(propertiesMap, scala.reflect.ClassTag$.MODULE$.apply(propertiesMap.getClass()));

		ruleRequiredMeasuresAvgMapBroadcast = sc.broadcast(ruleRegistryMeasuresAvgMap,
				scala.reflect.ClassTag$.MODULE$.apply(ruleRegistryMeasuresAvgMap.getClass()));

		ruleRequiredMeasuresAggrMapBroadcast = sc.broadcast(ruleRegistryMeasuresAggrMap,
				scala.reflect.ClassTag$.MODULE$.apply(ruleRegistryMeasuresAggrMap.getClass()));

		ruleRequiredMeasuresMapBroadcast = sc.broadcast(ruleRegistryMeasuresMap,
				scala.reflect.ClassTag$.MODULE$.apply(ruleRegistryMeasuresMap.getClass()));


		ruleRequiredTagsMapBroadcast = sc
				.broadcast(ruleRegistryTagsMap, scala.reflect.ClassTag$.MODULE$.apply(ruleRegistryTagsMap.getClass()));

		ruleRegistryMap.forEach((key, value) -> LOG.info("==>> ruleUidToRuleIdMapping {}, {}", key,  value));
		ruleRegistryMapBroadcast = sc
				.broadcast(ruleRegistryMap, scala.reflect.ClassTag$.MODULE$.apply(ruleRegistryMap.getClass()));

		ruleNamesMapping.forEach((key, value) -> LOG.info("==>> parametersMap {}, {}", key, value));
		ruleNamesMappingBroadcast = sc
				.broadcast(ruleNamesMapping, scala.reflect.ClassTag$.MODULE$.apply(ruleNamesMapping.getClass()));

		ruleRequiredAssetTypeMapBroadcast = sc
				.broadcast(ruleRequiredAssetType, scala.reflect.ClassTag$.MODULE$.apply(ruleRequiredAssetType.getClass()));

	}

	@Override public List<String> getListOfRequiredTags(List<String> ruleIds) {
		List<String> tagsList = new ArrayList<>();
		for (String ruleName : ruleNames) {
			LOG.info ("ruleName for getting the getListOfRequiredTags: {}", ruleName);
			List<String> requiresMap = ruleRequiredTagsMapBroadcast.value().get(ruleName);
			if(requiresMap != null) {
				tagsList.addAll(requiresMap);
			}
		}
		tagsList.forEach( e -> LOG.info("list of Required tags: {} ", e));
		return tagsList;
	}

	@Override public List<String> getListOfRequiredMeasures(List<String> ruleIds) {
		List<String> measuresList = new ArrayList<>();
		for (String ruleId : ruleNames) {
			LOG.info ("ruleId for getting the listOfRequiredMeasures: {}", ruleId);
			List<String> requiresMap = ruleRequiredMeasuresAvgMapBroadcast.value().get(ruleId);
			if(requiresMap != null) {
				measuresList.addAll(requiresMap);
			}
		}
		return measuresList;
	}

	@Override public List<String> getListOfRequiredMeasuresAvg(List<String> ruleIds) {
		List<String> measuresList = new ArrayList<>();
		for (String ruleName : ruleNames) {
			LOG.info ("ruleName for getting the listOfRequiredMeasuresAvg: {}", ruleName);
			List<String> requiresMap = ruleRequiredMeasuresAvgMapBroadcast.value().get(ruleName);
			if(requiresMap != null) {
				measuresList.addAll(requiresMap);
			} else {
				LOG.info("Measures map is null from rule: {}", ruleName);
			}
		}
		return measuresList;
	}

	@Override public List<String> getListOfRequiredMeasuresAggr(List<String> ruleIds) {
		List<String> measuresList = new ArrayList<>();
		for (String ruleName : ruleNames) {
			LOG.info ("ruleName for getting the listOfRequiredMeasuresAggr: {}", ruleName);
			List<String> requiresMap = ruleRequiredMeasuresAggrMapBroadcast.value().get(ruleName);
			if(requiresMap != null) {
				measuresList.addAll(requiresMap);
			}
		}
		return measuresList;
	}

	public List<String> getListOfRegisteredAssetsType(List<String> ruleIds) {
		List<String> assetsList = new ArrayList<>();
		for (String ruleName : ruleNames) {
			LOG.info ("ruleName for getting the getListOfRegisteredAssets: {}", ruleName);
			List<String> requiresMap = ruleRequiredAssetTypeMapBroadcast.value().get(ruleName);
			if(requiresMap != null) {
				assetsList.addAll(requiresMap);
			}
		}
		return assetsList;
	}

	public String getMaster() {
		// TODO Auto-generated method stub
		return propertiesBean.getValue(SparkConfigurations.SPARK_MASTER);
	}

	public PropertiesBean getPropertiesBean() {
		return propertiesBean;
	}

	@Override public String getDate() {
		return date;
	}

	@Override public void setDate(String date) {
		this.date = date;
	}


	public String getRuleNamesAsString() {
		return ruleNamesAsString;
	}

	@Override
	public void setRuleNamesAsString(String ruleNamesAsString) {

		this.ruleNamesAsString = ruleNamesAsString;
	}

	public List<String> getRuleUids() {
		return ruleNames;
	}

	@Override
	public void setRuleNames(List<String> rulesNames) {
		if(ruleNames == null) {
			ruleNames = new ArrayList<>();
		}
		for(String ruleName: rulesNames) {
			if(ruleName != null) {
				this.ruleNames.add(ruleName);
			}
		}
		LOG.info("RULE UIDS: {}", this.ruleNames);
	}

	@Override public String getRescrType() {
		return rescrType;
	}

	@Override public void setRescrType(String rescrType) {
		this.rescrType = rescrType;
	}

	@Override public String getGroupBy() {
		return this.groupBy;
	}

	@Override public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	@Override public String getEndTime() {
		return endTime;
	}

	@Override public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	@Override public String getTableName() {
		return tableName;
	}

	@Override public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override public String getTimeZone() {
		return this.timeZone;
	}

	@Override public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	@Override
	public void setAppName(String appName) {
		this.appName = appName;
	}

	@Override
	public String getAppName() {
		return this.appName;
	}
	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	@Override
	public String getTimeFrame() {
		return this.timeFrame;
	}

	@Override
	public void setTimeFrame(String timeFrame) {
		this.timeFrame = timeFrame;
	}

}
