//package com.ge.current.em.aggregation.batch;
//
//import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
//import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
//import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_CONNECTION_PORT;
//
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.TimeZone;
//import java.util.UUID;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import com.datastax.driver.core.utils.UUIDs;
//import com.ge.current.em.analytics.common.APMDataLoader;
//import org.apache.commons.lang.StringUtils;
//import org.apache.solr.client.solrj.SolrClient;
//import org.apache.solr.client.solrj.SolrQuery;
//import org.apache.solr.client.solrj.impl.HttpSolrClient;
//import org.apache.solr.client.solrj.request.QueryRequest;
//import org.apache.solr.client.solrj.response.QueryResponse;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.cassandra.CassandraSQLContext;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.PropertySource;
//import org.springframework.core.env.Environment;
//import org.springframework.stereotype.Component;
//import scala.collection.Seq;
//
//import com.ge.current.ie.analytics.SparkMQStream;
//import com.ge.current.ie.bean.PropertiesBean;
//import com.ge.current.ie.bean.SparkConfigurations;
//import com.ge.current.ie.model.nosql.EventsDailyLog;
//import com.ge.current.ie.model.nosql.EventsHourlyLog;
//import com.ge.current.ie.model.nosql.EventsMonthlyLog;
//
//@Component
//@PropertySource("classpath:application-${spring.profiles.active}.properties")
//@ComponentScan(basePackages = "com.ge.current")
//public class SparkCassandraKPITillDateBatch extends SparkMQStream{
//	private static final Logger logger = LoggerFactory.getLogger(SparkCassandraKPITillDateBatch.class);
//	private SparkConf sparkConf;
//
//	@Autowired
//	PropertiesBean propertiesBean;
//
//	@Autowired
//	transient Environment env;
//
//	@Autowired
//	APMDataLoader apmDataLoader;
//
//	@Value(value = "${solr.url}")
//	private String solrUrl;
//
//	@Value(value = "${solr.user}")
//	private String solrUser;
//
//	@Value(value = "${solr.password}")
//	private String solrPassword;
//
//	transient private SparkContext sc = null;
//	private CassandraSQLContext cassandraSQLContext = null;
//
//	private static int SOLR_RESULT_ROWS = 100000;
//
//	@Override
//	public void run(Environment env, Boolean useSsl, String MQProtocol) {
//		process(env);
//	}
//
//	public void process(Environment env) {
//		System.out.println(".... SparkCassandraKPITillDateBatch process method ....");
//
//		// TODO: aggregation depends on local timezone now, changes needed here ?
//		Calendar cal = Calendar.getInstance();
//		cal.setTimeZone(TimeZone.getTimeZone("PST"));
//		String dateStr = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(cal.getTime());
//		String yyyymmdd = "20161223";//dateStr.substring(0, 8);
//		int hour = 2;//Integer.parseInt(dateStr.substring(9, 11));
//		System.out.println("hour: " + hour);
//
//		System.out.println(".... Scheduler Started .... " + dateStr);
//
//		SparkConf sparkConf = new SparkConf().setMaster(propertiesBean.getValue(SparkConfigurations.SPARK_MASTER))
//				.setAppName("TillDateKPISummaryBatchJob");
//		sparkConf.set("spark.cassandra.connection.host",
//				propertiesBean.getValue(SparkConfigurations.CASSANDRA_CONNECTION_HOST));
//		sparkConf.set("spark.cassandra.connection.port", getPropertiesBean().getValue(CASSANDRA_CONNECTION_PORT));
//
//		System.out.println("sparkConf: " + sparkConf);
//
//		sc = new SparkContext(sparkConf);
//		System.out.println("sc" + sc);
//		CassandraSQLContext cassandraSQLContext = new CassandraSQLContext(sc);
//
//		tillDateKpiBatch(sc, cassandraSQLContext, yyyymmdd, hour);
//
//		sc.stop();
//
//		System.out.println("****** SCHEDULER ENDED ******");
//
//	}
//
//	private void tillDateKpiBatch(SparkContext sc, CassandraSQLContext cassandraSQLContext, String yyyymmdd, int hour) {
//
//		System.out.println(" ======= starting KPI Aggregation ===========");
//
//		int year = Integer.valueOf(yyyymmdd.substring(0, 4));
//		int month = Integer.valueOf(yyyymmdd.substring(4, 6));
//
//		SolrClient solr = new HttpSolrClient.Builder(solrUrl + "iepmaster_dev.events_hourly").build();
//
//		try {
//			SolrQuery solrQuery = new SolrQuery();
//			solrQuery.set("q", "event_bucket:asset* AND hour:" + hour + " AND yyyymmdd:" + yyyymmdd + "");
//			solrQuery.setRows(SOLR_RESULT_ROWS);
//			System.out.println("solQuery: " + solrQuery);
//			QueryRequest req = new QueryRequest(solrQuery);
//			req.setBasicAuthCredentials(solrUser, solrPassword);
//			QueryResponse response = req.process(solr);
//			System.out.println("response: " + response);
//			SolrDocumentList list = response.getResults();
//			HashMap<String, EventsHourlyLog> hourlyResourceMap = new HashMap<String, EventsHourlyLog>();
//			for (SolrDocument doc : list) {
//				System.out.println(doc);
//				EventsHourlyLog ehour = generateEventsHourlyLogObjectFromSolrDocument(doc);
//				System.out.println("ehour: " + ehour);
//				hourlyResourceMap.put(ehour.getResrc_uid(), ehour);
//			}
//
//			System.out.println("list: " + list.size());
//			System.out.println("hourlyResourceMap" + hourlyResourceMap);
//			computeHourlyKpi(sc, cassandraSQLContext, hourlyResourceMap, yyyymmdd, hour);
//			computeDailyKpi(sc, cassandraSQLContext, hourlyResourceMap, yyyymmdd);
//			computeMonthlyKpi(sc, cassandraSQLContext, hourlyResourceMap, year, month);
//		}
//
//		catch (Exception e) {
//			System.out.println("exception in tillDateKpiBatch method: " + e.getMessage());
//		}
//
//	}
//
//	private EventsHourlyLog generateEventsHourlyLogObjectFromSolrDocument(SolrDocument doc) {
//		if (doc != null) {
//			System.out.println(doc);
//			String event_bucket = (String) doc.getFieldValue("event_bucket");
//			String enterprise_uid = (String) doc.getFieldValue("enterprise_uid");
//			String cYyyymmdd = (String) doc.getFieldValue("yyyymmdd");
//			int hour = (int) doc.getFieldValue("hour");
//			String resrc_type = (String) doc.getFieldValue("resrc_type");
//			Date timestamp = (Date) doc.getFieldValue("event_ts");
//			String region_name = (String) doc.getFieldValue("region_name");
//			String resrc_uid = (String) doc.getFieldValue("resrc_uid");
//			String site_city = (String) doc.getFieldValue("site_city");
//			String site_country = (String) doc.getFieldValue("site_country");
//			String site_name = (String) doc.getFieldValue("site_name");
//			String site_state = (String) doc.getFieldValue("site_state");
//			List<String> labels = (List<String>) doc.get("labels");
//			List<String> segments = (List<String>) doc.get("segments");
//			List<String> zones = (List<String>) doc.get("zones");
//			HashMap<String, Double> measures_aggr = new HashMap<String, Double>();
//			Double measures_aggrKWh = (Double) doc.getFieldValue("measures_aggrkWh");
//			System.out.println("measures_aggrKWh: " + measures_aggrKWh);
//			if (measures_aggrKWh != null) {
//				measures_aggr.put("measures_aggrkWh", measures_aggrKWh);
//			}
//
//			UUID logUuid = UUIDs.timeBased();
//
//			EventsHourlyLog ehour = new EventsHourlyLog(event_bucket, enterprise_uid, cYyyymmdd, hour, logUuid,
//					resrc_uid, resrc_type, timestamp, region_name, site_city, site_state, site_country, site_name,
//					zones, segments, labels, measures_aggr, null, null, null, null, null, null);
//			return ehour;
//
//		}
//		return null;
//	}
//
//	private void computeHourlyKpi(SparkContext sc, CassandraSQLContext cassandraSQLContext,
//			HashMap<String, EventsHourlyLog> hourlyResourceMap, String yyyymmdd, int hour) {
//		List<EventsHourlyLog> updatedEhourList = new ArrayList<EventsHourlyLog>();
//		for (String resourceSourceKey : hourlyResourceMap.keySet()) {
//			EventsHourlyLog ehour = hourlyResourceMap.get(resourceSourceKey);
//			UUID logUuid = UUIDs.timeBased();
//			String kpiEventBucket = getKpiEventBucket(resourceSourceKey);
//			Double SqFt = apmDataLoader.generateSiteSqftMapping().get(resourceSourceKey);
//			Map<String,String> externalProps = new HashMap<String,String>();
//			Map<String,String> existingExtProps = ehour.getExt_props();
//			if(existingExtProps!=null && existingExtProps.size()>0){
//				existingExtProps.put("ext_propsSqFt",String.valueOf(SqFt));
//				externalProps.putAll(existingExtProps);
//			}else{
//				externalProps.put("ext_propsSqFt",String.valueOf(SqFt));
//			}
//
//			System.out.println("externalProps: " + externalProps);
//			EventsHourlyLog updatedEhour = new EventsHourlyLog(kpiEventBucket, ehour.getEnterprise_uid(),
//					ehour.getYyyymmdd(), ehour.getHour(), logUuid, ehour.getResrc_uid(), ehour.getResrc_type(),
//					ehour.getEvent_ts(), ehour.getRegion_name(), ehour.getSite_city(), ehour.getSite_state(),
//					ehour.getSite_country(), ehour.getSite_name(), ehour.getZones(), ehour.getSegments(),
//					ehour.getLabels(), ehour.getMeasures_aggr(), ehour.getMeasures_cnt(), ehour.getMeasures_min(),
//					ehour.getMeasures_max(), ehour.getMeasures_avg(), ehour.getTags(), externalProps);
//
//			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!  updatedEhour: " + updatedEhour);
//			System.out.println("========================================================");
//			System.out.println("========================================================");
//			updatedEhourList.add(updatedEhour);
//		}
//		Seq<EventsHourlyLog> seq = scala.collection.JavaConverters.asScalaIterableConverter(updatedEhourList).asScala()
//				.toSeq();
//		System.out.println("seq: " + seq);
//		JavaRDD<EventsHourlyLog> eventsHourlyRDD = sc
//				.parallelize(seq, 1, scala.reflect.ClassTag$.MODULE$.apply(EventsHourlyLog.class)).toJavaRDD();
//
//		List<EventsHourlyLog> l = eventsHourlyRDD.collect();
//		for (EventsHourlyLog e : l) {
//			System.out.println("onj : " + e);
//		}
//		System.out.println("eventsHourlyRDD: " + eventsHourlyRDD.count());
//		javaFunctions(eventsHourlyRDD).writerBuilder("iepmaster_dev", "events_hourly", mapToRow(EventsHourlyLog.class))
//				.saveToCassandra();
//	}
//
//	private void computeDailyKpi(SparkContext sc, CassandraSQLContext cassandraSQLContext,
//			HashMap<String, EventsHourlyLog> hourlyResourceMap, String yyyymmdd) {
//
//		List<EventsDailyLog> updatedEDayList = new ArrayList<EventsDailyLog>();
//
//		SolrClient solr = new HttpSolrClient.Builder(solrUrl + "iepmaster_dev.events_daily").build();
//
//		try {
//			SolrQuery solrQuery = new SolrQuery();
//			solrQuery.set("q", "event_bucket:kpi* AND yyyymmdd:" + yyyymmdd + "");
//			solrQuery.setRows(SOLR_RESULT_ROWS);
//			System.out.println("solQuery: " + solrQuery);
//			QueryRequest req = new QueryRequest(solrQuery);
//			req.setBasicAuthCredentials(solrUser, solrPassword);
//			QueryResponse response = req.process(solr);
//			System.out.println("response: " + response);
//			SolrDocumentList existingDailyList = response.getResults();
//			if (existingDailyList.size() > 0) {
//				for (SolrDocument doc : existingDailyList) {
//					System.out.println(doc);
//					String resourceSourceKey = (String) doc.getFieldValue("resrc_uid");
//					HashMap<String, Double> existingMeasuresAggr = new HashMap<String, Double>();
//					Double measures_aggrKWh = (Double) doc.getFieldValue("measures_aggrkWh");
//					if (measures_aggrKWh != null) {
//						existingMeasuresAggr.put("measures_aggrkWh", measures_aggrKWh);
//					}
//					System.out.println("measures_aggrKWh: " + measures_aggrKWh);
//
//					Map<String, Double> newMeasuresAggr = hourlyResourceMap.get(resourceSourceKey).getMeasures_aggr();
//					Map<String, Double> combinedMap = Stream
//							.concat(existingMeasuresAggr.entrySet().stream(), newMeasuresAggr.entrySet().stream())
//							.collect(Collectors.groupingBy(Map.Entry::getKey,
//									Collectors.summingDouble(Map.Entry::getValue)));
//
//					System.out.println("combinedMap: " + combinedMap);
//					String event_bucket = (String) doc.getFieldValue("event_bucket");
//					String enterprise_uid = (String) doc.getFieldValue("enterprise_uid");
//					String cYyyymmdd = (String) doc.getFieldValue("yyyymmdd");
//					Date timestamp = (Date) doc.getFieldValue("event_ts");
//					String resrc_type = (String) doc.getFieldValue("resrc_type");
//					String region_name = (String) doc.getFieldValue("region_name");
//					String resrc_uid = (String) doc.getFieldValue("resrc_uid");
//					String site_city = (String) doc.getFieldValue("site_city");
//					String site_country = (String) doc.getFieldValue("site_country");
//					String site_name = (String) doc.getFieldValue("site_name");
//					String site_state = (String) doc.getFieldValue("site_state");
//					List<String> labels = (List<String>) doc.get("labels");
//					List<String> segments = (List<String>) doc.get("segments");
//					List<String> zones = (List<String>) doc.get("zones");
//					Map<String, String> ext_props = (Map<String, String>)doc.get("ext_props");
//					UUID logUuid = UUIDs.timeBased();
//
//					Double SqFt = apmDataLoader.generateSiteSqftMapping().get(resourceSourceKey);
//
//					Map<String,String> externalProps = new HashMap<String,String>();
//					if(ext_props!=null && ext_props.size()>0){
//						ext_props.put("ext_propsSqFt",String.valueOf(SqFt));
//						externalProps.putAll(ext_props);
//					}else{
//						externalProps.put("ext_propsSqFt",String.valueOf(SqFt));
//					}
//
//					EventsDailyLog eDay = new EventsDailyLog(event_bucket, enterprise_uid, cYyyymmdd, logUuid,
//							timestamp, resrc_uid, resrc_type, region_name, site_city, site_state, site_country,
//							site_name, zones, segments, labels, combinedMap, null, null, null, null, null, externalProps);
//					System.out.println("eDay: " + eDay);
//					updatedEDayList.add(eDay);
//
//				}
//			} else {
//				for (String resourceSourceKey : hourlyResourceMap.keySet()) {
//					EventsHourlyLog ehour = hourlyResourceMap.get(resourceSourceKey);
//					UUID log_uuid = UUIDs.timeBased();
//					String kpiEventBucket = getKpiEventBucket(resourceSourceKey);
//					Double SqFt = apmDataLoader.generateSiteSqftMapping().get(resourceSourceKey);
//
//					Map<String,String> externalProps = new HashMap<String,String>();
//					Map<String,String> existingExtProps = ehour.getExt_props();
//					if(existingExtProps!=null && existingExtProps.size()>0){
//						existingExtProps.put("ext_propsSqFt",String.valueOf(SqFt));
//						externalProps.putAll(existingExtProps);
//					}else{
//						externalProps.put("ext_propsSqFt",String.valueOf(SqFt));
//					}
//					EventsDailyLog updatedEDay = new EventsDailyLog(kpiEventBucket, ehour.getEnterprise_uid(),
//							ehour.getYyyymmdd(), log_uuid, ehour.getEvent_ts(), ehour.getResrc_uid(),
//							ehour.getResrc_type(), ehour.getRegion_name(), ehour.getSite_city(), ehour.getSite_state(),
//							ehour.getSite_country(), ehour.getSite_name(), ehour.getZones(), ehour.getSegments(),
//							ehour.getLabels(), ehour.getMeasures_aggr(), ehour.getMeasures_cnt(),
//							ehour.getMeasures_min(), ehour.getMeasures_max(), ehour.getMeasures_avg(), ehour.getTags(),
//							externalProps);
//
//					System.out.println("updatedEDay: " + updatedEDay);
//					updatedEDayList.add(updatedEDay);
//				}
//			}
//
//		}
//
//		catch (Exception e) {
//			System.out.println("exception in computeDailyKpi method: " + e.getMessage());
//		}
//
//		Seq<EventsDailyLog> seq = scala.collection.JavaConverters.asScalaIterableConverter(updatedEDayList).asScala()
//				.toSeq();
//		JavaRDD<EventsDailyLog> eventsDailyRDD = sc
//				.parallelize(seq, 1, scala.reflect.ClassTag$.MODULE$.apply(EventsDailyLog.class)).toJavaRDD();
//
//		javaFunctions(eventsDailyRDD).writerBuilder("iepmaster_dev", "events_daily", mapToRow(EventsDailyLog.class))
//				.saveToCassandra();
//	}
//
//	private void computeMonthlyKpi(SparkContext sc, CassandraSQLContext cassandraSQLContext,
//			HashMap<String, EventsHourlyLog> hourlyResourceMap, int year, int month) {
//
//		List<EventsMonthlyLog> updatedEMonthList = new ArrayList<EventsMonthlyLog>();
//
//		SolrClient solr = new HttpSolrClient.Builder(solrUrl + "iepmaster_dev.events_monthly").build();
//
//		try {
//			SolrQuery solrQuery = new SolrQuery();
//			solrQuery.set("q", "event_bucket:kpi* AND yyyy:" + year + " AND month:" + month + "");
//			solrQuery.setRows(SOLR_RESULT_ROWS);
//			System.out.println("solQuery: " + solrQuery);
//			QueryRequest req = new QueryRequest(solrQuery);
//			req.setBasicAuthCredentials(solrUser, solrPassword);
//			QueryResponse response = req.process(solr);
//			System.out.println("response: " + response);
//			SolrDocumentList existingMonthlyList = response.getResults();
//			if (existingMonthlyList.size() > 0) {
//				for (SolrDocument doc : existingMonthlyList) {
//					System.out.println(doc);
//					String resourceSourceKey = (String) doc.getFieldValue("resrc_uid");
//					HashMap<String, Double> existingMeasuresAggr = new HashMap<String, Double>();
//					Double measures_aggrKWh = (Double) doc.getFieldValue("measures_aggrkWh");
//					System.out.println("measures_aggrKWh: " + measures_aggrKWh);
//					if (measures_aggrKWh != null) {
//						existingMeasuresAggr.put("measures_aggrkWh", measures_aggrKWh);
//					}
//
//					Map<String, Double> newMeasuresAggr = hourlyResourceMap.get(resourceSourceKey).getMeasures_aggr();
//					Map<String, Double> combinedMap = Stream
//							.concat(existingMeasuresAggr.entrySet().stream(), newMeasuresAggr.entrySet().stream())
//							.collect(Collectors.groupingBy(Map.Entry::getKey,
//									Collectors.summingDouble(Map.Entry::getValue)));
//
//					System.out.println("combinedMap: " + combinedMap);
//					String event_bucket = (String) doc.getFieldValue("event_bucket");
//					String enterprise_uid = (String) doc.getFieldValue("enterprise_uid");
//					int yyyy = (int) doc.getFieldValue("yyyy");
//					int m = (int) doc.getFieldValue("month");
//					String resrc_type = (String) doc.getFieldValue("resrc_type");
//					Date timestamp = (Date) doc.getFieldValue("event_ts");
//					String region_name = (String) doc.getFieldValue("region_name");
//					String resrc_uid = (String) doc.getFieldValue("resrc_uid");
//					String site_city = (String) doc.getFieldValue("site_city");
//					String site_country = (String) doc.getFieldValue("site_country");
//					String site_name = (String) doc.getFieldValue("site_name");
//					String site_state = (String) doc.getFieldValue("site_state");
//					List<String> labels = (List<String>) doc.get("labels");
//					List<String> segments = (List<String>) doc.get("segments");
//					List<String> zones = (List<String>) doc.get("zones");
//					UUID logUuid = UUIDs.timeBased();
//					Map<String, String> ext_props = (Map<String, String>)doc.get("ext_props");
//					Double SqFt = apmDataLoader.generateSiteSqftMapping().get(resourceSourceKey);
//
//					Map<String,String> externalProps = new HashMap<String,String>();
//					if(ext_props!=null && ext_props.size()>0){
//						ext_props.put("ext_propsSqFt",String.valueOf(SqFt));
//						externalProps.putAll(ext_props);
//					}else{
//						externalProps.put("ext_propsSqFt",String.valueOf(SqFt));
//					}
//
//					EventsMonthlyLog updatedEMonth = new EventsMonthlyLog(event_bucket, enterprise_uid, yyyy, month,
//							logUuid, timestamp, resrc_uid, resrc_type, region_name, site_city, site_state, site_country,
//							site_name, zones, segments, labels, combinedMap, null, null, null, null, null, externalProps);
//					System.out.println("updatedEMonth: " + updatedEMonth);
//					updatedEMonthList.add(updatedEMonth);
//
//				}
//			} else {
//				for (String resourceSourceKey : hourlyResourceMap.keySet()) {
//					EventsHourlyLog ehour = hourlyResourceMap.get(resourceSourceKey);
//					String date = ehour.getYyyymmdd();
//					int yyyy = Integer.valueOf(date.substring(0, 4));
//					int mm = Integer.valueOf(date.substring(4, 6));
//					UUID log_uuid = UUIDs.timeBased();
//					String kpiEventBucket = getKpiEventBucket(resourceSourceKey);
//					Double SqFt = apmDataLoader.generateSiteSqftMapping().get(resourceSourceKey);
//					Map<String,String> externalProps = new HashMap<String,String>();
//					Map<String,String> existingExtProps = ehour.getExt_props();
//					if(existingExtProps!=null && existingExtProps.size()>0){
//						existingExtProps.put("ext_propsSqFt",String.valueOf(SqFt));
//						externalProps.putAll(existingExtProps);
//					}else{
//						externalProps.put("ext_propsSqFt",String.valueOf(SqFt));
//					}
//					EventsMonthlyLog updatedEMonth = new EventsMonthlyLog(kpiEventBucket, ehour.getEnterprise_uid(),
//							yyyy, month, log_uuid, ehour.getEvent_ts(), ehour.getResrc_uid(), ehour.getResrc_type(),
//							ehour.getRegion_name(), ehour.getSite_city(), ehour.getSite_state(),
//							ehour.getSite_country(), ehour.getSite_name(), ehour.getZones(), ehour.getSegments(),
//							ehour.getLabels(), ehour.getMeasures_aggr(), ehour.getMeasures_cnt(),
//							ehour.getMeasures_min(), ehour.getMeasures_max(), ehour.getMeasures_avg(), ehour.getTags(),
//							externalProps);
//					System.out.println("updatedEMonth: " + updatedEMonth);
//					updatedEMonthList.add(updatedEMonth);
//				}
//			}
//
//		}
//
//		catch (Exception e) {
//			System.out.println("exception in computeMonthlyKpi method: " + e.getMessage());
//		}
//
//		Seq<EventsMonthlyLog> seq = scala.collection.JavaConverters.asScalaIterableConverter(updatedEMonthList)
//				.asScala().toSeq();
//		JavaRDD<EventsMonthlyLog> eventsMonthlyRDD = sc
//				.parallelize(seq, 1, scala.reflect.ClassTag$.MODULE$.apply(EventsMonthlyLog.class)).toJavaRDD();
//
//		javaFunctions(eventsMonthlyRDD)
//				.writerBuilder("iepmaster_dev", "events_monthly", mapToRow(EventsMonthlyLog.class)).saveToCassandra();
//	}
//
//	private String getAssetEventBucket(String sourceKey) {
//		if (!StringUtils.isEmpty(sourceKey)) {
//			return "asset." + sourceKey + ".reading";
//		}
//		return null;
//	}
//
//	private String getKpiEventBucket(String sourceKey) {
//		if (!StringUtils.isEmpty(sourceKey)) {
//			return "kpi." + sourceKey + ".reading";
//		}
//		return null;
//	}
//
//}
