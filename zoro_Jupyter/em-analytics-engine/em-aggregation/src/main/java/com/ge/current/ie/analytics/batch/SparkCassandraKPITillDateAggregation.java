package com.ge.current.ie.analytics.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import com.ge.current.em.analytics.common.EmailService;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.entities.analytics.APMMappings;
import com.ge.current.em.entities.analytics.HourlyKpiInfoBean;
import com.ge.current.ie.model.nosql.EventsDailyLog;
import com.ge.current.ie.model.nosql.EventsDailyStaging;
import com.ge.current.ie.model.nosql.EventsHourlyLog;
import com.ge.current.ie.model.nosql.EventsMonthlyLog;
import com.ge.current.ie.model.nosql.EventsMonthlyStaging;

import scala.collection.Seq;

@Component
public class SparkCassandraKPITillDateAggregation implements Serializable {

	private static final Logger logger = LoggerFactory.getLogger(SparkCassandraKPITillDateAggregation.class);

	static APMMappings APMMappings = null;
	static CassandraSQLContext cassandraSQLContext = null;
	static SparkContext sparkContext = null;
	static String keyspace = null;
	@Autowired
	transient EmailService emailService;

	public void tillDateKpiBatch(JavaRDD<EventsHourlyLog> eventsHourlyRDD, APMMappings apmMappings, SparkContext sc,
			String KeySpace) {

		if (eventsHourlyRDD != null && eventsHourlyRDD.count() > 0) {
			logger.info(" =============   =============   ============= ");
			logger.info(" =============   =============   ============= ");
			logger.info(" ============= Hourly RDD passed for till date kpi calculation ============= ");
			logger.info(" =============   =============   ============= ");
			logger.info(" =============   =============   ============= ");

			List<EventsHourlyLog> recentHourlyObjs = eventsHourlyRDD.collect();
			APMMappings = apmMappings;
			cassandraSQLContext = new CassandraSQLContext(sc);
			sparkContext = sc;
			keyspace = KeySpace;
			logger.info(" **** LOAD TYPE SEGMENTS ****" + APMMappings.getLoadTypeSegments());
			logger.info(" **** LOAD TYPE SEGMENTS NAMES ****" + APMMappings.getLoadTypeSegmentNameMapping());
			logger.info(" **** SITE INSTALLATION YEARS ****" + APMMappings.getSiteInstallationYearsMapping());
			logger.info(" **** SITE OCCUPANCY DETAILS ****" + APMMappings.getSiteOccupancyMapping());
			logger.info(" **** SITE PEAK HOURS DETAILS ****" + APMMappings.getSitePeakHoursMapping());
			logger.info(" **** SITE SQFT DETAILS ****" + APMMappings.getSiteSqftMapping());
			logger.info(" **** SITE TIMEZONE DETAILS ****" + APMMappings.getSiteTimeZoneMapping());
			logger.info(" **** SEGMENT SITE MAPPING DETAILS ****" + APMMappings.getSegmentSiteMapping());
			logger.info(" **** SEGMENT ENTERPRISE MAPPING DETAILS ****" + APMMappings.getSegmentEnterpriseMapping());
			logger.info(" **** SITE ENTERPRISE MAPPING DETAILS ****" + APMMappings.getSiteEnterpriseMapping());

			if((APMMappings.getLoadTypeSegments()==null || APMMappings.getLoadTypeSegments().isEmpty())
					|| (APMMappings.getLoadTypeSegmentNameMapping()==null ||APMMappings.getLoadTypeSegmentNameMapping().isEmpty())
					|| (APMMappings.getSiteInstallationYearsMapping()==null ||APMMappings.getSiteInstallationYearsMapping().isEmpty())
					|| (APMMappings.getSiteOccupancyMapping()==null || APMMappings.getSiteOccupancyMapping().isEmpty())
					|| (APMMappings.getSitePeakHoursMapping()==null || APMMappings.getSitePeakHoursMapping().isEmpty())
					|| (APMMappings.getSiteSqftMapping()==null || APMMappings.getSiteSqftMapping().isEmpty())
					|| (APMMappings.getSiteTimeZoneMapping()==null || APMMappings.getSiteTimeZoneMapping().isEmpty())
					|| (APMMappings.getSegmentSiteMapping()==null || APMMappings.getSegmentSiteMapping().isEmpty())
					|| (APMMappings.getSegmentEnterpriseMapping()==null || APMMappings.getSegmentEnterpriseMapping().isEmpty())
					|| (APMMappings.getSiteEnterpriseMapping()==null || APMMappings.getSiteEnterpriseMapping().isEmpty())) {
				Map<String, String> configErrorMap = new HashMap<>();
				configErrorMap.put("errorMessage","APM setup failed. RequiredMaps empty and proceeding");
				emailService.sendMessage("EM Hourly KPI Spark Job",
						sc.applicationId(), configErrorMap, "APM setup failed. RequiredMaps empty",
						"EM Hourly KPI Spark Job broadcast Alert.");

			}

			HashMap<String, HourlyKpiInfoBean> hourlyResourceMap = generateKPIEventsHourlyLogFromRDD(recentHourlyObjs,sc);

			if (hourlyResourceMap != null && hourlyResourceMap.size() > 0) {
				// every entry should have same yyyymmdd and hour as it is per
				// timezone
				Map.Entry<String, HourlyKpiInfoBean> entry = hourlyResourceMap.entrySet().iterator().next();
				HourlyKpiInfoBean hourInfoBean = entry.getValue();
				EventsHourlyLog hourObj = hourInfoBean.getCurrHour();
				String yyyymmdd = hourObj.getYyyymmdd();
				int hour = hourObj.getHour();

				computeHourlyKpi(hourlyResourceMap);

				logger.info(" =============   =============   ============= ");
				logger.info(" completed till date aggregation to hourly table for yyyymmdd: " + yyyymmdd + " and hour: "
						+ hour + "");
				logger.info(" =============   =============   ============= ");

				computeDailyKpi(hourlyResourceMap, yyyymmdd);
				logger.info(" =============   =============   ============= ");
				logger.info(" completed till date aggregation to daily table for yyyymmdd: " + yyyymmdd + "");
				logger.info(" =============   =============   ============= ");

				computeMonthlyKpi(hourlyResourceMap, Integer.parseInt(yyyymmdd.substring(0, 4)),
						Integer.parseInt(yyyymmdd.substring(4, 6)));
				logger.info(" =============   =============   ============= ");
				logger.info(" completed till date aggregation to monthly table for yyyy: " + yyyymmdd.substring(0, 4)
						+ " and month: " + yyyymmdd.substring(4, 6));
				logger.info(" =============   =============   ============= ");

			}

		}
	}

	private HashMap<String, HourlyKpiInfoBean> generateKPIEventsHourlyLogFromRDD(
			List<EventsHourlyLog> recentHourlyObjs, SparkContext sc) {

		HashMap<String, HourlyKpiInfoBean> hourlyResourceMap = new HashMap<String, HourlyKpiInfoBean>();

		Set<String> loadTypeSegments = APMMappings.getLoadTypeSegments();
		for (EventsHourlyLog recentHourlyObj : recentHourlyObjs) {
			String resrc_type = recentHourlyObj.getResrc_type();
			String resrc_uid = recentHourlyObj.getResrc_uid();
			if ("SITE".equalsIgnoreCase(resrc_type)) {
				generateKPIEventsHourlyForSite(hourlyResourceMap, recentHourlyObj,sc);
			} else if (loadTypeSegments != null && loadTypeSegments.contains(resrc_uid)) {
				generateKPIEventsHourlyForSegment(hourlyResourceMap, recentHourlyObj);
			}
		}

		return hourlyResourceMap;

	}

	private void generateKPIEventsHourlyForSite(HashMap<String, HourlyKpiInfoBean> hourlyResourceMap,
			EventsHourlyLog recentHourlyObj,SparkContext sc) {

		String siteSourceKey = recentHourlyObj.getResrc_uid();
		String resourceType = recentHourlyObj.getResrc_type();
		String yyyymmdd = recentHourlyObj.getYyyymmdd();
		int hour = recentHourlyObj.getHour();

		HashMap<String, Double> measuresAggr = ((HashMap<String, Double>) recentHourlyObj.getMeasures_aggr() == null)
				? new HashMap<String, Double>() : (HashMap<String, Double>) recentHourlyObj.getMeasures_aggr();

		// saving only kpi related aggr fields
		HashMap<String, Double> kpiMeasuresAggr = new HashMap<String, Double>();
		Double kWh = measuresAggr.get("measures_aggrkWh");
		if (kWh == null) {
			kWh = 0.0;
			//alert
			Map<String, String> configErrorMap = new HashMap<>();
			configErrorMap.put("errorMessage","kWh is null setting to 0.0 and proceeding");
			emailService.sendMessage("EM Hourly KPI Spark Job",
					sc.applicationId(), configErrorMap, "measures_aggrkWh is null",
					"EM Hourly KPI Spark Job broadcast Alert.");
		}
		kpiMeasuresAggr.put("measures_aggrkWh", kWh);
		boolean isOccupied = isOccupied(siteSourceKey, yyyymmdd, hour);
		if (isOccupied) {
			kpiMeasuresAggr.put("measures_aggrOccupiedkWh", kWh);
			kpiMeasuresAggr.put("measures_aggrUnOccupiedkWh", 0.0);
		} else {
			kpiMeasuresAggr.put("measures_aggrOccupiedkWh", 0.0);
			kpiMeasuresAggr.put("measures_aggrUnOccupiedkWh", kWh);
		}

		String peakInfo = null;
		if (APMMappings.getSitePeakHoursMapping() != null) {
			if (APMMappings.getSitePeakHoursMapping().get(siteSourceKey) != null) {
				peakInfo = APMMappings.getSitePeakHoursMapping().get(siteSourceKey).get(hour);
			}
		}
		logger.info(" peak info is: " + peakInfo);
		if ("MidPeak".equalsIgnoreCase(peakInfo)) {
			kpiMeasuresAggr.put("measures_aggrOffPeakkWh", 0.0);
			kpiMeasuresAggr.put("measures_aggrMidPeakkWh", kWh);
			kpiMeasuresAggr.put("measures_aggrPeakkWh", 0.0);
		} else if ("Peak".equalsIgnoreCase(peakInfo)) {
			kpiMeasuresAggr.put("measures_aggrOffPeakkWh", 0.0);
			kpiMeasuresAggr.put("measures_aggrMidPeakkWh", 0.0);
			kpiMeasuresAggr.put("measures_aggrPeakkWh", kWh);
		} else {
			kpiMeasuresAggr.put("measures_aggrOffPeakkWh", kWh);
			kpiMeasuresAggr.put("measures_aggrMidPeakkWh", 0.0);
			kpiMeasuresAggr.put("measures_aggrPeakkWh", 0.0);
		}

		HashMap<String, String> externalProps = ((HashMap<String, String>) recentHourlyObj.getExt_props() == null)
				? new HashMap<String, String>() : (HashMap<String, String>) recentHourlyObj.getExt_props();

		Map<String, Double> siteSqftMapping = (APMMappings.getSiteSqftMapping() == null) ? new HashMap<String, Double>()
				: APMMappings.getSiteSqftMapping();

		externalProps.put("ext_propsSqFt", String.valueOf(siteSqftMapping.get(siteSourceKey)));

		HashMap<String, Double> measuresMax = ((HashMap<String, Double>) recentHourlyObj.getMeasures_max() == null)
				? new HashMap<String, Double>() : (HashMap<String, Double>) recentHourlyObj.getMeasures_max();

		// saving only kpi related max fields
		HashMap<String, Double> kpiMeasuresMax = new HashMap<String, Double>();
		Double kW = measuresMax.get("measures_maxzoneElecMeterPowerSensor");
		if (kW == null) {
			kW = 0.0;
			//alert
			Map<String, String> configErrorMap = new HashMap<>();
			configErrorMap.put("errorMessage","kW is null setting to 0.0 and proceeding");

			emailService.sendMessage("EM Hourly KPI Spark Job",
					sc.applicationId(), configErrorMap, "measures_maxzoneElecMeterPowerSensor is null",
					"EM Hourly KPI Spark Job broadcast Alert.");
		}
		kpiMeasuresMax.put("measures_maxzoneElecMeterPowerSensor", kW);

		// savings 0th element - prevYearKWh & 1st element - installatedYearkWh
		Double[] savings = getHourlyPrevAndInstallYearValues(siteSourceKey, "SITE", yyyymmdd, hour);
		// yoy savings
		if(savings[0] != null){
		    Double YOYkWhSavings = savings[0] - kWh;
	        kpiMeasuresAggr.put("measures_aggrYOYkWhSavings", YOYkWhSavings);
		}
		// lifetimekWh savings
		if(savings[1] != null){
		    Double lifeTimekWhSavings = savings[1] - kWh;
	        kpiMeasuresAggr.put("measures_aggrLifeTimekWhSavings", lifeTimekWhSavings);
		}
		
		EventsHourlyLog currHour = new EventsHourlyLog(getKpiEventBucket(siteSourceKey, resourceType, APMMappings),
				recentHourlyObj.getEnterprise_uid(), yyyymmdd, hour, UUIDs.timeBased(), siteSourceKey,
				recentHourlyObj.getResrc_type(), recentHourlyObj.getEvent_ts(), recentHourlyObj.getRegion_name(),
				recentHourlyObj.getSite_city(), recentHourlyObj.getSite_state(), recentHourlyObj.getSite_country(),
				recentHourlyObj.getSite_name(), recentHourlyObj.getZones(), recentHourlyObj.getSegments(),
				recentHourlyObj.getLabels(), kpiMeasuresAggr, null, null, kpiMeasuresMax, null, null, externalProps);

		HashMap<String, Double> oldHourValues = getOldHourlyObjkWh(currHour);
		logger.info(" !!!!!!!!!!! oldHourValues: " + oldHourValues);
		HourlyKpiInfoBean hourlykpiInfo;
		if (oldHourValues == null) {
			hourlykpiInfo = new HourlyKpiInfoBean(currHour, false, null, null, null, null, null, null, null);
		} else {
		    hourlykpiInfo = new HourlyKpiInfoBean(currHour, true,
		            oldHourValues.get("measures_aggrkWh"),
		            oldHourValues.get("measures_maxzoneElecMeterPowerSensor"),
		            oldHourValues.get("measures_aggrMidPeakkWh"),
		            oldHourValues.get("measures_aggrPeakkWh"),
		            oldHourValues.get("measures_aggrOffPeakkWh"),
		            oldHourValues.get("measures_aggrOccupiedkWh"),
		            oldHourValues.get("measures_aggrUnOccupiedkWh"));
		}

		hourlyResourceMap.put(siteSourceKey, hourlykpiInfo);
	}

	private HashMap<String, Double> getOldHourlyObjkWh(EventsHourlyLog currHour) {

		String oldHourlyObjQuery = "select  measures_aggr, measures_max, resrc_uid,yyyymmdd,hour from  " + keyspace
				+ ".events_hourly where " + " event_bucket= '" + currHour.getEvent_bucket()
				+ "' and enterprise_uid =  '" + currHour.getEnterprise_uid() + "'" + " and yyyymmdd = '"
				+ currHour.getYyyymmdd() + "' and hour =" + currHour.getHour();
		HashMap<String, Double> oldHourMap = extractOldHourlykWhValue(oldHourlyObjQuery);
		return oldHourMap;
	}

	private boolean isOccupied(String resourceKey, String yyyymmdd, int hour) {

		Map<String, HashMap<String, Integer[]>> siteOccupancyMap = APMMappings.getSiteOccupancyMapping();
		if (siteOccupancyMap != null && siteOccupancyMap.size() > 0) {
			if (!siteOccupancyMap.containsKey(resourceKey)) {
				return false;
			} else {
				Calendar cal = Calendar
						.getInstance(TimeZone.getTimeZone(APMMappings.getSiteTimeZoneMapping().get(resourceKey)));
				cal.set(Integer.parseInt(yyyymmdd.substring(0, 4)), Integer.parseInt(yyyymmdd.substring(4, 6)) - 1,
						Integer.parseInt(yyyymmdd.substring(6, 8)));
				Integer dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
				Integer[] occupiedHours = null;
				// TODO: not checking holidays in siteOccupancyMap,
				// no service to validate given date if its a holiday or not
				if (2 <= dayOfWeek && dayOfWeek <= 6) {
					occupiedHours = siteOccupancyMap.get(resourceKey).get("weekdays");
				} else {
					occupiedHours = siteOccupancyMap.get(resourceKey).get("weekends");
				}
				if (occupiedHours != null && occupiedHours[0] <= hour && hour < occupiedHours[1]) {
					return true;
				}

			}
		}

		return false;
	}

	private void generateKPIEventsHourlyForSegment(HashMap<String, HourlyKpiInfoBean> hourlyResourceMap,
			EventsHourlyLog recentHourlyObj) {

		HashMap<String, Double> measuresAggr = ((HashMap<String, Double>) recentHourlyObj.getMeasures_aggr() == null)
				? new HashMap<String, Double>() : (HashMap<String, Double>) recentHourlyObj.getMeasures_aggr();
		Double kWh = measuresAggr.get("measures_aggrkWh");
		if (kWh == null) {
			kWh = 0.0;
		}

		// saving only kpi related aggr fields
		HashMap<String, Double> kpiMeasuresAggr = new HashMap<String, Double>();
		kpiMeasuresAggr.put("measures_aggrkWh", kWh);

		HashMap<String, String> externalProps = ((HashMap<String, String>) recentHourlyObj.getExt_props() == null)
				? new HashMap<String, String>() : (HashMap<String, String>) recentHourlyObj.getExt_props();
		
		Map<String, String>	segmentNames = APMMappings.getLoadTypeSegmentNameMapping();
		if(segmentNames != null && segmentNames.size()>0){
		    externalProps.put("ext_propsName",segmentNames.get(recentHourlyObj.getResrc_uid()));
		}

		EventsHourlyLog currHour = new EventsHourlyLog(
				getKpiEventBucket(recentHourlyObj.getResrc_uid(), recentHourlyObj.getResrc_type(), APMMappings),
				recentHourlyObj.getEnterprise_uid(), recentHourlyObj.getYyyymmdd(), recentHourlyObj.getHour(),
				UUIDs.timeBased(), recentHourlyObj.getResrc_uid(), recentHourlyObj.getResrc_type(),
				recentHourlyObj.getEvent_ts(), recentHourlyObj.getRegion_name(), recentHourlyObj.getSite_city(),
				recentHourlyObj.getSite_state(), recentHourlyObj.getSite_country(), recentHourlyObj.getSite_name(),
				recentHourlyObj.getZones(), recentHourlyObj.getSegments(), recentHourlyObj.getLabels(), kpiMeasuresAggr,
				null, null, null, null, null, externalProps);

		HashMap<String, Double> oldHourValues = getOldHourlyObjkWh(currHour);
        logger.info(" !!!!!!!!!!! oldHourValues: " + oldHourValues);
        HourlyKpiInfoBean hourlykpiInfo;
        if (oldHourValues == null) {
            hourlykpiInfo = new HourlyKpiInfoBean(currHour, false, null, null, null, null, null, null, null);
        } else {
            hourlykpiInfo = new HourlyKpiInfoBean(currHour, true,
                    oldHourValues.get("measures_aggrkWh"),
                    oldHourValues.get("measures_maxzoneElecMeterPowerSensor"),
                    oldHourValues.get("measures_aggrMidPeakkWh"),
                    oldHourValues.get("measures_aggrPeakkWh"),
                    oldHourValues.get("measures_aggrOffPeakkWh"),
                    oldHourValues.get("measures_aggrOccupiedkWh"),
                    oldHourValues.get("measures_aggrUnOccupiedkWh"));
        }
		hourlyResourceMap.put(recentHourlyObj.getResrc_uid(), hourlykpiInfo);
	}

	private void computeHourlyKpi(HashMap<String, HourlyKpiInfoBean> hourlyResourceMap) {
		List<EventsHourlyLog> updatedEhourList = new ArrayList<EventsHourlyLog>();
		for (HourlyKpiInfoBean bean : hourlyResourceMap.values()) {
			updatedEhourList.add(bean.getCurrHour());
		}

		Seq<EventsHourlyLog> seqH = scala.collection.JavaConverters.asScalaIterableConverter(updatedEhourList).asScala()
				.toSeq();
		JavaRDD<EventsHourlyLog> eventsHourlyRDD = sparkContext
				.parallelize(seqH, 1, scala.reflect.ClassTag$.MODULE$.apply(EventsHourlyLog.class)).toJavaRDD();
		javaFunctions(eventsHourlyRDD).writerBuilder(keyspace, "events_hourly", mapToRow(EventsHourlyLog.class))
				.saveToCassandra();

		// return updatedEhourList;
	}

	private Double[] getHourlyPrevAndInstallYearValues(String resourceSourceKey, String resourceType, String yyyymmdd,
			int hour) {

		String eventBucket = getRegularEventBucket(resourceSourceKey, resourceType);
		String enterpriseUid = getEnterpriseUid(resourceSourceKey, resourceType);

		String prevYearDate = String.valueOf(Integer.parseInt(yyyymmdd.substring(0, 4)) - 1) + yyyymmdd.substring(4, 8);
		String prevYearQuery = "select  measures_aggr,resrc_uid,yyyymmdd,hour from  " + keyspace
				+ ".events_hourly where " + " event_bucket= '" + eventBucket + "' and enterprise_uid =  '"
				+ enterpriseUid + "'" + " and yyyymmdd = '" + prevYearDate + "' and hour =" + hour;
		Double prevYearkWh = extractKwhValue(prevYearQuery);

		Double baseYearkWh = null;
		if(APMMappings.getSiteInstallationYearsMapping() != null){
		    Integer siteInstallationYear = APMMappings.getSiteInstallationYearsMapping().get(resourceSourceKey);
	        if(siteInstallationYear != null){
	            String baseYearDate = String.valueOf(siteInstallationYear - 1) + yyyymmdd.substring(4, 8);
	            String baseYearQuery = "select  measures_aggr,resrc_uid,yyyymmdd,hour from  " + keyspace
	                    + ".events_hourly where " + " event_bucket= '" + eventBucket + "' and enterprise_uid =  '"
	                    + enterpriseUid + "'" + " and yyyymmdd = '" + baseYearDate + "' and hour =" + hour;
	            baseYearkWh  = extractKwhValue(baseYearQuery);
	        }
		}
		
		/*String baseYearDate = String.valueOf(APMMappings.getSiteInstallationYearsMapping().get(resourceSourceKey) - 1)
				+ yyyymmdd.substring(4, 8);
		*/

		Double[] values = { prevYearkWh, baseYearkWh };

		return values;
	}

	private void computeDailyKpi(HashMap<String, HourlyKpiInfoBean> hourlyResourceMap, String yyyymmdd) {
		List<EventsDailyStaging> updatedEDayStgList = new ArrayList<EventsDailyStaging>();

		String dailyStgEventBucket = yyyymmdd.substring(0, 6) + "." + yyyymmdd.substring(6, 8) + "." + "kpi";

		String exisitingDailyStgQuery = "select event_bucket,yyyymm,day,resrc_type,resrc_uid,event_ts,enterprise_uid,measures_aggr,measures_max,ext_props from  "
				+ keyspace + ".events_daily_stg" + " where " + " event_bucket= '" + dailyStgEventBucket + "'"
				+ " and yyyymm = '" + yyyymmdd.substring(0, 6) + "' and day ="
				+ Integer.parseInt(yyyymmdd.substring(6, 8));

		HashMap<String, EventsDailyStaging> existingDailyStgMap = generateExistingKPIDailyStgMap(
				exisitingDailyStgQuery);
		logger.info("================ hourlyResourceMap: " + hourlyResourceMap);
		logger.info("================ existingDailyStgMap: " + existingDailyStgMap);

		for (String resourceSourceKey : hourlyResourceMap.keySet()) {
			logger.info("================ each");
			HourlyKpiInfoBean hourlyKpiInfoBean = hourlyResourceMap.get(resourceSourceKey);
			logger.info("================ hourlyKpiInfoBean: " + hourlyKpiInfoBean);
			EventsHourlyLog newHourlyObj = hourlyKpiInfoBean.getCurrHour();
			logger.info("================ newHourlyObj: " + newHourlyObj);
			String resourceType = newHourlyObj.getResrc_type();

			// computing yoy kWh savings, Lifetime kWh savings and peakDemand
			// for only sites
			Double prevYearkWh = null;
			Double installedYearkWh = null;
			Double currkWMax = 0.0;

			if ("site".equalsIgnoreCase(resourceType)) {
				Double[] savings = getDailyPrevAndInstallYearValues(resourceSourceKey, resourceType, yyyymmdd);
				prevYearkWh = savings[0];
				installedYearkWh = savings[1];
				currkWMax = newHourlyObj.getMeasures_max().get("measures_maxzoneElecMeterPowerSensor");
			}

			if (existingDailyStgMap.containsKey(resourceSourceKey)) {
				// update the kpi entries
				EventsDailyStaging existingDailyStgObj = existingDailyStgMap.get(resourceSourceKey);
				HashMap<String, Double> existingMeasuresAggr = (HashMap<String, Double>) existingDailyStgObj
						.getMeasures_aggr();
				Map<String, Double> newMeasuresAggr = newHourlyObj.getMeasures_aggr();

				// handling out of sync and re-run
				if (hourlyKpiInfoBean.isUpdated() && hourlyKpiInfoBean.getOldkWh() != null) {
					logger.info("===== inside out of sync =======");
					Double oldHourlykWhValue = hourlyKpiInfoBean.getOldkWh();
					logger.info("===== oldHourlyValue :" + oldHourlykWhValue);
					Double oldDailykWhValue = existingMeasuresAggr.get("measures_aggrkWh");
					logger.info("===== oldDailyValue :" + oldDailykWhValue);
					existingMeasuresAggr.put("measures_aggrkWh", (oldDailykWhValue - oldHourlykWhValue));
					if ("site".equalsIgnoreCase(resourceType)) {
					    existingMeasuresAggr.put("measures_aggrMidPeakkWh", (existingMeasuresAggr.get("measures_aggrMidPeakkWh") - hourlyKpiInfoBean.getOldMidPeakkWh()));
	                    existingMeasuresAggr.put("measures_aggrOffPeakkWh", (existingMeasuresAggr.get("measures_aggrOffPeakkWh") - hourlyKpiInfoBean.getOldOffPeakkWh()));
	                    existingMeasuresAggr.put("measures_aggrPeakkWh", (existingMeasuresAggr.get("measures_aggrPeakkWh") - hourlyKpiInfoBean.getOldPeakkWh()));
	                    existingMeasuresAggr.put("measures_aggrOccupiedkWh", (existingMeasuresAggr.get("measures_aggrOccupiedkWh") - hourlyKpiInfoBean.getOldOccupiedkWh()));
	                    existingMeasuresAggr.put("measures_aggrUnOccupiedkWh", (existingMeasuresAggr.get("measures_aggrUnOccupiedkWh") - hourlyKpiInfoBean.getOldUnOccupiedkWh()));
					}
					
					logger.info("===== existingMeasuresAggr :" + existingMeasuresAggr);
					// kW is handled automatically
				}

				HashMap<String, Double> combinedMeasuresAggr = new HashMap<String, Double>();
				Set<String> keys = new HashSet(existingMeasuresAggr.keySet());
				keys.addAll(newMeasuresAggr.keySet());
				for(String key: keys){
				   // can sum up 
				   if(existingMeasuresAggr.containsKey(key) && newMeasuresAggr.containsKey(key)){
				       combinedMeasuresAggr.put(key, existingMeasuresAggr.get(key) + newMeasuresAggr.get(key));
				   }else if(existingMeasuresAggr.containsKey(key)){
				       combinedMeasuresAggr.put(key, existingMeasuresAggr.get(key));
				   }else if(newMeasuresAggr.containsKey(key)){
				       combinedMeasuresAggr.put(key, newMeasuresAggr.get(key));
				   }
				    
				}
				
				logger.info("===== combinedMeasuresAggr :" + combinedMeasuresAggr);

				HashMap<String, Double> measuresMax = (HashMap<String, Double>) existingDailyStgObj.getMeasures_max();
				if ("site".equalsIgnoreCase(resourceType)) {
					// calculate YOY kWh savings
					Double currentkWh = (combinedMeasuresAggr.get("measures_aggrkWh") == null) ? 0.0
							: combinedMeasuresAggr.get("measures_aggrkWh");
					if(prevYearkWh != null){
					    Double YOYkWhSavings = prevYearkWh - currentkWh;
	                    combinedMeasuresAggr.put("measures_aggrYOYkWhSavings", YOYkWhSavings);
					}
					
					// calculate Lifetime kWh savings
					if(installedYearkWh != null){
					    Double lifeTimekWhSavings = installedYearkWh - currentkWh;
	                    combinedMeasuresAggr.put("measures_aggrLifeTimekWhSavings", lifeTimekWhSavings);
					}
					
					// calculate peakDemand
					Double existingkWMax = (measuresMax.get("measures_maxzoneElecMeterPowerSensor") == null) ? 0.0
							: measuresMax.get("measures_maxzoneElecMeterPowerSensor");
					Double kWMax = (currkWMax > existingkWMax) ? currkWMax : existingkWMax;
					measuresMax.put("measures_maxzoneElecMeterPowerSensor", kWMax);
				}

				UUID logUuid = UUIDs.timeBased();

				EventsDailyStaging updatedDailyStgObj = new EventsDailyStaging(existingDailyStgObj.getEvent_bucket(),
						existingDailyStgObj.getYyyymm(), existingDailyStgObj.getDay(),
						existingDailyStgObj.getResrc_type(), existingDailyStgObj.getResrc_uid(), logUuid,
						newHourlyObj.getEvent_ts(), existingDailyStgObj.getEnterprise_uid(), null, null, null, null,
						null, null, null, null, combinedMeasuresAggr, null, null, measuresMax, null, null,
						existingDailyStgObj.getExt_props());

				logger.info(" updated daily stg kpi object:  " + updatedDailyStgObj);
				updatedEDayStgList.add(updatedDailyStgObj);
			} else {
				if ("site".equalsIgnoreCase(resourceType)) {
					// calculate YOY kWh savings
					Double currentkWh = newHourlyObj.getMeasures_aggr().get("measures_aggrkWh");
					if(prevYearkWh != null){
					    Double YOYkWhSavings = prevYearkWh - currentkWh;
	                    newHourlyObj.getMeasures_aggr().put("measures_aggrYOYkWhSavings", YOYkWhSavings);
                    }
					// calculate LifeTime kWh savings
					if(installedYearkWh != null){
					    Double lifeTimekWhSavings = installedYearkWh - currentkWh;
	                    newHourlyObj.getMeasures_aggr().put("measures_aggrLifeTimekWhSavings", lifeTimekWhSavings);
					}
					
				}
				UUID log_uuid = UUIDs.timeBased();

				EventsDailyStaging updatedDailyStgObj = new EventsDailyStaging(dailyStgEventBucket,
						newHourlyObj.getYyyymmdd().substring(0, 6),
						Integer.parseInt(newHourlyObj.getYyyymmdd().substring(6, 8)), newHourlyObj.getResrc_type(),
						newHourlyObj.getResrc_uid(), log_uuid, newHourlyObj.getEvent_ts(),
						newHourlyObj.getEnterprise_uid(), null, null, null, null, null, null, null, null,
						newHourlyObj.getMeasures_aggr(), null, null, newHourlyObj.getMeasures_max(), null, null,
						newHourlyObj.getExt_props());

				logger.info(" new daily stg kpi object:  " + updatedDailyStgObj);
				updatedEDayStgList.add(updatedDailyStgObj);

			}

		}

		Seq<EventsDailyStaging> seq = scala.collection.JavaConverters.asScalaIterableConverter(updatedEDayStgList)
				.asScala().toSeq();
		JavaRDD<EventsDailyStaging> eventsDailyStgRDD = sparkContext
				.parallelize(seq, 1, scala.reflect.ClassTag$.MODULE$.apply(EventsDailyStaging.class)).toJavaRDD();
		javaFunctions(eventsDailyStgRDD).writerBuilder(keyspace, "events_daily_stg", mapToRow(EventsDailyStaging.class))
				.saveToCassandra();

		logger.info("===== wrote in daily stg for resourceSourceKey: " + "yyyymmdd: " + yyyymmdd + " time: "
				+ System.currentTimeMillis());

		APMMappings tempMappings = APMMappings;

		JavaRDD<EventsDailyLog> eventsDailyLogRDD = eventsDailyStgRDD
				.map(new Function<EventsDailyStaging, EventsDailyLog>() {

					@Override
					public EventsDailyLog call(EventsDailyStaging event) throws Exception {
						EventsDailyLog dailyLog = new EventsDailyLog();
						BeanUtils.copyProperties(dailyLog, event);
						dailyLog.setEvent_bucket(
								getKpiEventBucket(event.getResrc_uid(), event.getResrc_type(), tempMappings));
						return dailyLog;
					}
				});

		javaFunctions(eventsDailyLogRDD).writerBuilder(keyspace, "events_daily", mapToRow(EventsDailyLog.class))
				.saveToCassandra();

		logger.info("===== wrote in daily log for resourceSourceKey: " + "yyyymmdd: " + yyyymmdd + " time: "
				+ System.currentTimeMillis());

		// return updatedEDayStgList;

	}

	private HashMap<String, EventsDailyStaging> generateExistingKPIDailyStgMap(String dailyStgQuery) {

		HashMap<String, EventsDailyStaging> map = new HashMap<String, EventsDailyStaging>();
		DataFrame dataFrame = cassandraSQLContext.cassandraSql(dailyStgQuery).cache();
		dataFrame.show();
		if (dataFrame.count() > 0) {
			List<Row> listRows = dataFrame.toJavaRDD().collect();
			for (Row row : listRows) {
				EventsDailyStaging eventsDailyStaging = new EventsDailyStaging(row.getString(0), row.getString(1),
						row.getInt(2), row.getString(3), row.getString(4), UUIDs.timeBased(),
						(java.util.Date) row.get(5), row.getString(6), null, null, null, null, null, null, null, null,
						(row.get(7) == null) ? new HashMap<String, Double>()
								: new HashMap<String, Double>(row.getJavaMap(7)),
						null, null,
						(row.get(8) == null) ? new HashMap<String, Double>()
								: new HashMap<String, Double>(row.getJavaMap(8)),
						null, null, (row.get(9) == null) ? new HashMap<String, String>()
								: new HashMap<String, String>(row.getJavaMap(9)));

				logger.info("putting eventsDailyStaging: " + eventsDailyStaging.toString());
				map.put(row.getString(4), eventsDailyStaging);
			}
		}
		return map;
	}

	private Double[] getDailyPrevAndInstallYearValues(String resourceSourceKey, String resourceType, String yyyymmdd) {
		String eventBucket = getRegularEventBucket(resourceSourceKey, resourceType);
		String enterpriseUid = getEnterpriseUid(resourceSourceKey, resourceType);

		String prevYearDate = String.valueOf(Integer.parseInt(yyyymmdd.substring(0, 4)) - 1) + yyyymmdd.substring(4, 8);
		String prevYearQuery = "select  measures_aggr,resrc_uid,yyyymm,day from  " + keyspace + ".events_daily where "
				+ " event_bucket= '" + eventBucket + "' and enterprise_uid =  '" + enterpriseUid + "'"
				+ " and yyyymm = '" + prevYearDate.substring(0, 6) + "' and day ="
				+ Integer.parseInt(prevYearDate.substring(6, 8));
		Double prevYearkWh = extractKwhValue(prevYearQuery);

		Double baseYearkWh = null;
		if(APMMappings.getSiteInstallationYearsMapping() != null){
		    Integer siteInstallationYear = APMMappings.getSiteInstallationYearsMapping().get(resourceSourceKey);
	        if(siteInstallationYear != null){
	            String baseYearDate = String.valueOf(siteInstallationYear - 1) + yyyymmdd.substring(4, 8);
	            String baseYearQuery = "select  measures_aggr,resrc_uid,yyyymm,day from  " + keyspace + ".events_daily where "
	                    + " event_bucket= '" + eventBucket + "' and enterprise_uid =  '" + enterpriseUid + "'"
	                    + " and yyyymm = '" + baseYearDate.substring(0, 6) + "' and day ="
	                    + Integer.parseInt(baseYearDate.substring(6, 8));
	            baseYearkWh  = extractKwhValue(baseYearQuery);
	        } 
		}
		/*String baseYearDate = String.valueOf(APMMappings.getSiteInstallationYearsMapping().get(resourceSourceKey) - 1)
				+ yyyymmdd.substring(4, 8);
		String baseYearQuery = "select  measures_aggr,resrc_uid,yyyymm,day from  " + keyspace + ".events_daily where "
				+ " event_bucket= '" + eventBucket + "' and enterprise_uid =  '" + enterpriseUid + "'"
				+ " and yyyymm = '" + baseYearDate.substring(0, 6) + "' and day ="
				+ Integer.parseInt(baseYearDate.substring(6, 8));
		Double baseYearkWh = extractKwhValue(baseYearQuery);*/

		Double[] values = { prevYearkWh, baseYearkWh };
		return values;
	}

	private void computeMonthlyKpi(HashMap<String, HourlyKpiInfoBean> hourlyResourceMap, int year, int month) {
		List<EventsMonthlyStaging> updatedEMonthStgList = new ArrayList<EventsMonthlyStaging>();

		String monthlyStgEventBucket = String.valueOf(year) + "." + String.valueOf(month) + "." + "kpi";

		String exisitingMonthlyStgQuery = "select event_bucket,yyyy,month,resrc_type,resrc_uid,event_ts,enterprise_uid,measures_aggr,measures_max,ext_props from  "
				+ keyspace + ".events_monthly_stg" + " where " + " event_bucket= '" + monthlyStgEventBucket + "'"
				+ " and yyyy = '" + year + "' and month =" + month;

		HashMap<String, EventsMonthlyStaging> existingMonthlyStgMap = generateExistingKPIMonthlyStgMap(
				exisitingMonthlyStgQuery);

		for (String resourceSourceKey : hourlyResourceMap.keySet()) {
			HourlyKpiInfoBean hourlyKpiInfoBean = hourlyResourceMap.get(resourceSourceKey);
			EventsHourlyLog newHourlyObj = hourlyKpiInfoBean.getCurrHour();
			String resourceType = newHourlyObj.getResrc_type();

			Double installedYearkWh = null;
			Double prevYearkWh = null;
			Double currkWMax = 0.0;
			// computing yoy kWh savings, Lifetime kWh savings and peakDemand
			// for only sites
			if ("site".equalsIgnoreCase(resourceType)) {
				Double[] savings = getMonthlyPrevAndInstallYearValues(resourceSourceKey, resourceType, year, month);
				prevYearkWh = savings[0];
				installedYearkWh = savings[1];
				currkWMax = newHourlyObj.getMeasures_max().get("measures_maxzoneElecMeterPowerSensor");
			}
			if (existingMonthlyStgMap.containsKey(resourceSourceKey)) {
				EventsMonthlyStaging existingMonthlyStgObj = existingMonthlyStgMap.get(resourceSourceKey);
				HashMap<String, Double> existingMeasuresAggr = (HashMap<String, Double>) existingMonthlyStgObj
						.getMeasures_aggr();
				Map<String, Double> newMeasuresAggr = newHourlyObj.getMeasures_aggr();

				// handling out of sync and re-run
				if (hourlyKpiInfoBean.isUpdated() && hourlyKpiInfoBean.getOldkWh() != null) {
				    logger.info("===== inside out of sync =======");
                    Double oldHourlykWhValue = hourlyKpiInfoBean.getOldkWh();
                    logger.info("===== oldHourlyValue :" + oldHourlykWhValue);
                    Double oldDailykWhValue = existingMeasuresAggr.get("measures_aggrkWh");
                    logger.info("===== oldDailyValue :" + oldDailykWhValue);
                    existingMeasuresAggr.put("measures_aggrkWh", (oldDailykWhValue - oldHourlykWhValue));
                    if ("site".equalsIgnoreCase(resourceType)) {
                        existingMeasuresAggr.put("measures_aggrMidPeakkWh", (existingMeasuresAggr.get("measures_aggrMidPeakkWh") - hourlyKpiInfoBean.getOldMidPeakkWh()));
                        existingMeasuresAggr.put("measures_aggrOffPeakkWh", (existingMeasuresAggr.get("measures_aggrOffPeakkWh") - hourlyKpiInfoBean.getOldOffPeakkWh()));
                        existingMeasuresAggr.put("measures_aggrPeakkWh", (existingMeasuresAggr.get("measures_aggrPeakkWh") - hourlyKpiInfoBean.getOldPeakkWh()));
                        existingMeasuresAggr.put("measures_aggrOccupiedkWh", (existingMeasuresAggr.get("measures_aggrOccupiedkWh") - hourlyKpiInfoBean.getOldOccupiedkWh()));
                        existingMeasuresAggr.put("measures_aggrUnOccupiedkWh", (existingMeasuresAggr.get("measures_aggrUnOccupiedkWh") - hourlyKpiInfoBean.getOldUnOccupiedkWh()));
                    }
					// kW is handled automatically
				}

				HashMap<String, Double> combinedMeasuresAggr = new HashMap<String, Double>();
                Set<String> keys = new HashSet(existingMeasuresAggr.keySet());
                keys.addAll(newMeasuresAggr.keySet());
                for(String key: keys){
                   // can sum up 
                   if(existingMeasuresAggr.containsKey(key) && newMeasuresAggr.containsKey(key)){
                       combinedMeasuresAggr.put(key, existingMeasuresAggr.get(key) + newMeasuresAggr.get(key));
                   }else if(existingMeasuresAggr.containsKey(key)){
                       combinedMeasuresAggr.put(key, existingMeasuresAggr.get(key));
                   }else if(newMeasuresAggr.containsKey(key)){
                       combinedMeasuresAggr.put(key, newMeasuresAggr.get(key));
                   }
                    
                }

				HashMap<String, Double> measuresMax = (HashMap<String, Double>) existingMonthlyStgObj.getMeasures_max();

				if ("site".equalsIgnoreCase(resourceType)) {
					// calculate YOY kWh savings
					Double currentkWh = (combinedMeasuresAggr.get("measures_aggrkWh") == null) ? 0.0
							: combinedMeasuresAggr.get("measures_aggrkWh");
					if(prevYearkWh != null){
					    Double YOYkWhSavings = prevYearkWh - currentkWh;
	                    combinedMeasuresAggr.put("measures_aggrYOYkWhSavings", YOYkWhSavings);
					}
					
					// calculate Lifetime kWh savings
					if(installedYearkWh != null){
					    Double lifeTimekWhSavings = installedYearkWh - currentkWh;
	                    combinedMeasuresAggr.put("measures_aggrLifeTimekWhSavings", lifeTimekWhSavings);
					}
					
					// calculate peakDemand
					Double existingkWMax = (measuresMax.get("measures_maxzoneElecMeterPowerSensor") == null) ? 0.0
							: measuresMax.get("measures_maxzoneElecMeterPowerSensor");
					Double kWMax = (currkWMax > existingkWMax) ? currkWMax : existingkWMax;
					measuresMax.put("measures_maxzoneElecMeterPowerSensor", kWMax);
				}
				UUID logUuid = UUIDs.timeBased();
				// retaining original timestamp of the object -
				// existingDailyObj.getEvent_ts()
				EventsMonthlyStaging updatedMonthlyStgObj = new EventsMonthlyStaging(monthlyStgEventBucket,
						existingMonthlyStgObj.getYyyy(), existingMonthlyStgObj.getMonth(),
						existingMonthlyStgObj.getResrc_type(), existingMonthlyStgObj.getResrc_uid(), logUuid,
						newHourlyObj.getEvent_ts(), existingMonthlyStgObj.getEnterprise_uid(), null, null, null, null,
						null, null, null, null, combinedMeasuresAggr, null, null, measuresMax, null, null,
						existingMonthlyStgObj.getExt_props());

				logger.info(" updated monthly stg kpi object:  " + updatedMonthlyStgObj);
				updatedEMonthStgList.add(updatedMonthlyStgObj);
			} else {
				if ("site".equalsIgnoreCase(resourceType)) {
					// calculate YOY kWh savings
					Double currentkWh = (newHourlyObj.getMeasures_aggr().get("measures_aggrkWh") == null) ? 0.0
							: newHourlyObj.getMeasures_aggr().get("measures_aggrkWh");
					if(prevYearkWh != null){
					    Double YOYkWhSavings = prevYearkWh - currentkWh;
	                    newHourlyObj.getMeasures_aggr().put("measures_aggrYOYkWhSavings", YOYkWhSavings);
					}
					
					// calculate LifeTime kWh savings
					if(installedYearkWh != null){
					    Double lifeTimekWhSavings = installedYearkWh - currentkWh;
	                    newHourlyObj.getMeasures_aggr().put("measures_aggrLifeTimekWhSavings", lifeTimekWhSavings);
					}
					
				}
				UUID log_uuid = UUIDs.timeBased();
				String date = newHourlyObj.getYyyymmdd();
				int yyyy = Integer.valueOf(date.substring(0, 4));
				int mm = Integer.valueOf(date.substring(4, 6));

				EventsMonthlyStaging updatedMonthlyStgObj = new EventsMonthlyStaging(monthlyStgEventBucket, yyyy, mm,
						newHourlyObj.getResrc_type(), newHourlyObj.getResrc_uid(), log_uuid, newHourlyObj.getEvent_ts(),
						newHourlyObj.getEnterprise_uid(), null, null, null, null, null, null, null, null,
						newHourlyObj.getMeasures_aggr(), null, null, newHourlyObj.getMeasures_max(), null, null,
						newHourlyObj.getExt_props());

				logger.info(" new monthly stg kpi object:  " + updatedMonthlyStgObj);
				updatedEMonthStgList.add(updatedMonthlyStgObj);
			}
		}

		Seq<EventsMonthlyStaging> seq = scala.collection.JavaConverters.asScalaIterableConverter(updatedEMonthStgList)
				.asScala().toSeq();
		JavaRDD<EventsMonthlyStaging> eventsMonthlyStgRDD = sparkContext
				.parallelize(seq, 1, scala.reflect.ClassTag$.MODULE$.apply(EventsMonthlyStaging.class)).toJavaRDD();
		javaFunctions(eventsMonthlyStgRDD)
				.writerBuilder(keyspace, "events_monthly_stg", mapToRow(EventsMonthlyStaging.class)).saveToCassandra();

		logger.info("===== wrote in monthly stg for resourceSourceKey: " + "yyyy: " + year + " month: " + month
				+ " time: " + System.currentTimeMillis());

		APMMappings tempMappings = APMMappings;

		JavaRDD<EventsMonthlyLog> eventsMonthlyLogRDD = eventsMonthlyStgRDD
				.map(new Function<EventsMonthlyStaging, EventsMonthlyLog>() {

					@Override
					public EventsMonthlyLog call(EventsMonthlyStaging event) throws Exception {
						EventsMonthlyLog monthlyLog = new EventsMonthlyLog();
						BeanUtils.copyProperties(monthlyLog, event);
						monthlyLog.setEnterprise_uid(event.getEnterprise_uid());
						monthlyLog.setEvent_bucket(
								getKpiEventBucket(event.getResrc_uid(), event.getResrc_type(), tempMappings));
						return monthlyLog;
					}
				});

		javaFunctions(eventsMonthlyLogRDD).writerBuilder(keyspace, "events_monthly", mapToRow(EventsMonthlyLog.class))
				.saveToCassandra();

		logger.info("===== wrote in monthly log for resourceSourceKey: " + "yyyy: " + year + " month: " + month
				+ " time: " + System.currentTimeMillis());

		// return updatedEMonthStgList;

	}

	private HashMap<String, EventsMonthlyStaging> generateExistingKPIMonthlyStgMap(String monthlyStgQuery) {
		HashMap<String, EventsMonthlyStaging> map = new HashMap<String, EventsMonthlyStaging>();
		DataFrame dataFrame = cassandraSQLContext.cassandraSql(monthlyStgQuery).cache();
		dataFrame.show();
		if (dataFrame.count() > 0) {
			List<Row> listRows = dataFrame.toJavaRDD().collect();
			for (Row row : listRows) {
				EventsMonthlyStaging eventsMonthlyStaging = new EventsMonthlyStaging(row.getString(0), row.getInt(1),
						row.getInt(2), row.getString(3), row.getString(4), UUIDs.timeBased(),
						(java.util.Date) row.get(5), row.getString(6), null, null, null, null, null, null, null, null,
						(row.get(7) == null) ? new HashMap<String, Double>()
								: new HashMap<String, Double>(row.getJavaMap(7)),
						null, null,
						(row.get(8) == null) ? new HashMap<String, Double>()
								: new HashMap<String, Double>(row.getJavaMap(8)),
						null, null, (row.get(9) == null) ? new HashMap<String, String>()
								: new HashMap<String, String>(row.getJavaMap(9)));

				logger.info("putting eventsMonthlyStaging: " + eventsMonthlyStaging.toString());
				map.put(row.getString(4), eventsMonthlyStaging);
			}
		}
		return map;
	}

	private Double[] getMonthlyPrevAndInstallYearValues(String resourceSourceKey, String resourceType, int year,
			int month) {
		String eventBucket = getRegularEventBucket(resourceSourceKey, resourceType);
		String enterpriseUid = getEnterpriseUid(resourceSourceKey, resourceType);

		String prevYearQuery = "select  measures_aggr,resrc_uid,yyyy,month from  " + keyspace + ".events_monthly where "
				+ " event_bucket= '" + eventBucket + "' and enterprise_uid =  '" + enterpriseUid + "'" + " and yyyy = '"
				+ (year - 1) + "' and month =" + month;
		Double prevYearkWh = extractKwhValue(prevYearQuery);

		Double baseYearkWh = null;
		if(APMMappings.getSiteInstallationYearsMapping() != null){
		    Integer siteInstallationYear = APMMappings.getSiteInstallationYearsMapping().get(resourceSourceKey);
	        if(siteInstallationYear != null){
	            String baseYear = String.valueOf(APMMappings.getSiteInstallationYearsMapping().get(resourceSourceKey) - 1);
	            String baseYearQuery = "select  measures_aggr,resrc_uid,yyyy,month from  " + keyspace + ".events_monthly where "
	                    + " event_bucket= '" + eventBucket + "' and enterprise_uid =  '" + enterpriseUid + "'" + " and yyyy = '"
	                    + baseYear + "' and month =" + month;
	            baseYearkWh  = extractKwhValue(baseYearQuery);
	        }
	        
		}
		/*String baseYear = String.valueOf(APMMappings.getSiteInstallationYearsMapping().get(resourceSourceKey) - 1);
		String baseYearQuery = "select  measures_aggr,resrc_uid,yyyy,month from  " + keyspace + ".events_monthly where "
				+ " event_bucket= '" + eventBucket + "' and enterprise_uid =  '" + enterpriseUid + "'" + " and yyyy = '"
				+ baseYear + "' and month =" + month;
		Double baseYearkWh = extractKwhValue(baseYearQuery);*/

		Double[] values = { prevYearkWh, baseYearkWh };
		return values;
	}

	private String getKpiEventBucket(String sourceKey, String resourceType, APMMappings apmMappings) {
		if (!StringUtils.isEmpty(sourceKey) && !StringUtils.isEmpty(resourceType)) {
			if ("site".equalsIgnoreCase(resourceType)) {
				return "kpi" + "." + sourceKey + "." + "reading";
			} else {
			    if(apmMappings.getSegmentSiteMapping() != null){
			        String siteSourceKey = apmMappings.getSegmentSiteMapping().get(sourceKey);
	                return "kpi" + "." + siteSourceKey + "." + "LT" + "." + sourceKey + "." + "reading";
			    }
			}

		}
		return null;
	}

	private String getRegularEventBucket(String resourceKey, String resourceType) {
		if (!StringUtils.isEmpty(resourceKey) && !StringUtils.isEmpty(resourceType)) {
			// format : <timezone>.<resourceType>.<resourceKey>.reading
			if ("SITE".equalsIgnoreCase(resourceType)) {
			    if(APMMappings.getSiteTimeZoneMapping() != null){
			        String resourceTimeZone = APMMappings.getSiteTimeZoneMapping().get(resourceKey);
	                return resourceTimeZone + "." + resourceType + "." + resourceKey + "." + "reading";
			    }
			} else if ("SEGMENT".equalsIgnoreCase(resourceType)) {
			    if(APMMappings.getSegmentSiteMapping() != null && APMMappings.getSiteTimeZoneMapping() != null){
			        String siteSourceKey = APMMappings.getSegmentSiteMapping().get(resourceKey);
	                String resourceTimeZone = APMMappings.getSiteTimeZoneMapping().get(siteSourceKey);
	                return resourceTimeZone + "." + resourceType + "." + resourceKey + "." + "reading";
			    }
			}
		}
		return null;
	}

	private String getEnterpriseUid(String resourceSourceKey, String resourceType) {
		String enterpriseUid = null;
		if ("SEGMENT".equalsIgnoreCase(resourceType)) {
		    if(APMMappings.getSegmentEnterpriseMapping() != null){
		        enterpriseUid = APMMappings.getSegmentEnterpriseMapping().get(resourceSourceKey);
		    }
		} else if ("SITE".equalsIgnoreCase(resourceType)) {
		    if(APMMappings.getSiteEnterpriseMapping() != null){
		        enterpriseUid = APMMappings.getSiteEnterpriseMapping().get(resourceSourceKey);
		    }
		}
		return enterpriseUid;
	}

	private Double extractKwhValue(String historicalQuery) {

		logger.info("=== historicalQuery is " + historicalQuery);

		Double oldkWh = null;

		DataFrame historicalDataFrame = cassandraSQLContext.cassandraSql(historicalQuery).cache();
		historicalDataFrame.show();
		if (historicalDataFrame.count() > 0) {
			List<Row> histList = historicalDataFrame.toJavaRDD().collect();
			Map<String, Double> prevMap = (histList.get(0).getJavaMap(0) == null) ? new HashMap<String, Double>()
					: histList.get(0).getJavaMap(0);
			oldkWh = prevMap.get("measures_aggrkWh");
		}

		return oldkWh;
	}

	private HashMap<String, Double> extractOldHourlykWhValue(String oldQuery) {
	    HashMap<String, Double> oldValues = null;
		DataFrame historicalDataFrame = cassandraSQLContext.cassandraSql(oldQuery).cache();
		historicalDataFrame.show();
		if (historicalDataFrame.count() > 0) {
			List<Row> histList = historicalDataFrame.toJavaRDD().collect();
			oldValues = new HashMap<String, Double>();
			if (!histList.get(0).isNullAt(0)) {
			    Map<String, Double> prevAggrMap = histList.get(0).getJavaMap(0);
			    if(prevAggrMap!=null && prevAggrMap.size()>0){
			        oldValues.put("measures_aggrkWh", prevAggrMap.containsKey("measures_aggrkWh") ? prevAggrMap.get("measures_aggrkWh") : 0.0);
	                oldValues.put("measures_aggrMidPeakkWh", prevAggrMap.containsKey("measures_aggrMidPeakkWh") ? prevAggrMap.get("measures_aggrMidPeakkWh") : 0.0);
	                oldValues.put("measures_aggrPeakkWh", prevAggrMap.containsKey("measures_aggrPeakkWh") ? prevAggrMap.get("measures_aggrPeakkWh") : 0.0);
	                oldValues.put("measures_aggrOffPeakkWh", prevAggrMap.containsKey("measures_aggrOffPeakkWh") ? prevAggrMap.get("measures_aggrOffPeakkWh") : 0.0);
	                oldValues.put("measures_aggrUnOccupiedkWh", prevAggrMap.containsKey("measures_aggrUnOccupiedkWh") ? prevAggrMap.get("measures_aggrUnOccupiedkWh") : 0.0);
	                oldValues.put("measures_aggrOccupiedkWh", prevAggrMap.containsKey("measures_aggrOccupiedkWh") ? prevAggrMap.get("measures_aggrOccupiedkWh") : 0.0);
			    }
			}
			if (!histList.get(0).isNullAt(1)) {
			    Map<String, Double> prevMaxMap = histList.get(0).getJavaMap(1);
			    if(prevMaxMap!=null && prevMaxMap.size()>0){
			        logger.info("===== prevMaxMap: " + prevMaxMap);
			        oldValues.put("measures_maxzoneElecMeterPowerSensor", prevMaxMap.containsKey("measures_maxzoneElecMeterPowerSensor") ? prevMaxMap.get("measures_maxzoneElecMeterPowerSensor") : 0.0); 
			    }
            }
		}

		return oldValues;
	}

}
