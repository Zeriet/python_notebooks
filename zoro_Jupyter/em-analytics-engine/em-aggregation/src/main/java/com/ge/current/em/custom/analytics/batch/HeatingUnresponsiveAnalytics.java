//package com.ge.current.em.custom.analytics.batch;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.sql.cassandra.CassandraSQLContext;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.core.env.Environment;
//
//import com.datastax.spark.connector.japi.CassandraJavaUtil;
//import com.datastax.spark.connector.japi.CassandraRow;
//import com.ge.current.em.entities.analytics.AHUBean;
//import com.ge.current.em.entities.analytics.RulesBaseFact;
//import com.ge.current.ie.analytics.batch.ISparkBatch;
//import com.ge.current.ie.analytics.rules.IERulesUtil;
//import com.ge.current.ie.bean.EventLogStatus;
//import com.ge.current.ie.bean.PropertiesBean;
//import com.ge.current.ie.bean.SparkConfigurations;
//import com.ge.current.ie.model.nosql.EventLog;
//
//import scala.Tuple2;
//
//public class HeatingUnresponsiveAnalytics implements ISparkBatch, Serializable{
//
//	private static final long serialVersionUID = 1L;
//
//	private static final Logger LOG = LoggerFactory.getLogger(HeatingUnresponsiveAnalytics.class);
//
//	@Autowired
//	private PropertiesBean propertiesBean;
//
//	@Value("${spring.profiles.active}")
//	private String profile;
//
//	private CassandraSQLContext cassandraSQLContext = null;
//
//	public Iterable<EventLog> processPartition(Iterator<EventLog> events) {
//		List<EventLog> processedEvents = new ArrayList<>();
//
//		if(Boolean.getBoolean(propertiesBean.getValue(SparkConfigurations.EVENT_RULES_ENABLED))) {
//			IERulesUtil.initRulesEngine(propertiesBean);
//		}
//
//		events.forEachRemaining(rawEvent -> {
//			if (rawEvent == null) {
//				LOG.error("Received a null message to consume");
//				return;
//			}
//
//			// EventLogStatus.conditionMet is set to true
//			// if the eventlog met the rule condition.
//			EventLogStatus eventProcessingStatus = processEventWithRules(rawEvent);
//			System.out.println("Rule condition met for asset_id: " + eventProcessingStatus.getEventLog().getAssetUid() + " ,logUUid: " +eventProcessingStatus.getEventLog().getLogUuid() + ", measure: " + eventProcessingStatus.getEventLog().getMeasures().get("zoneAirTempSensor") + " ,conditionMet: "   + eventProcessingStatus.isConditionMet());
//			processedEvents.add(eventProcessingStatus.getEventLog());
//		});
//
//		return processedEvents;
//	}
//
//
//	public EventLogStatus processEventWithRules(Object rawEvents) {
//		if(rawEvents == null) return null;
//
//		EventLog eventLog = (EventLog) rawEvents;
//
//		EventLogStatus eventRuleStatus = new EventLogStatus(eventLog, false);
//		if(Boolean.getBoolean(propertiesBean.getValue(SparkConfigurations.EVENT_RULES_ENABLED))) {
//			LOG.info("Applying rules to the event");
//			IERulesUtil.applyRule(eventRuleStatus);
//		}
//
//		// event status reflecting the status of the object after applying the rules.
//		return eventRuleStatus;
//	}
//
//	private String aggregateLightingRule(String ruleName) {
//		return "";
//	}
//
//	private void sendEventsToRules(JavaRDD<RulesBaseFact> eventLogs, PropertiesBean propertiesBean) {
//
//	}
//
//	@Override
//	public void broadcast(SparkContext sc, PropertiesBean propertiesBean) {
//
//	}
//
// 	@Override
//	public void run(Environment arg0, String startTime, String endTime) {
//		// TODO Auto-generated method stub
//		LOG.info("SparkMQStream run method");
//		LOG.info("In SparkMQStream instance: master: {}, appName: {}, cassandraHostName: {}, spark batchInterval: {} ",
//				propertiesBean.getValue(SparkConfigurations.SPARK_MASTER), propertiesBean.getValue(SparkConfigurations.SPARK_APP_NAME), propertiesBean.getValue(SparkConfigurations.CASSANDRA_CONNECTION_HOST),
//				propertiesBean.getValue(SparkConfigurations.BATCH_INTERVAL));
//
//		SparkConf sparkConf = new SparkConf().setMaster(propertiesBean.getValue(SparkConfigurations.SPARK_MASTER))
//				.setAppName(propertiesBean.getValue(SparkConfigurations.SPARK_APP_NAME));
//		sparkConf.set("cassandra.connection.host", propertiesBean.getValue(SparkConfigurations.CASSANDRA_CONNECTION_HOST));
//		sparkConf.set("spark.streaming.unpersist", "false");
//		sparkConf.set("spark.driver.allowMultipleContexts", "true");
//
//		LOG.info("Using Properties ... " + propertiesBean.toString());
//		SparkContext sc = new SparkContext(sparkConf);
//
//		JavaRDD<CassandraRow> testInputTableRdd = CassandraJavaUtil.javaFunctions(sc).cassandraTable(propertiesBean.getValue(SparkConfigurations.CASSANDRA_KEYSPACE), "event_log").limit(10l);
//		cassandraSQLContext = new CassandraSQLContext(sc);
//		//		DataFrame dataFrame = cassandraSQLContext.cassandraSql("select * from events_15mins where event_bucket='20161108'");
//		//		GroupedData groupedData = dataFrame.groupBy("asset_uid","yyyymmdd");
//
//		JavaRDD<EventLog> rulesFact = testInputTableRdd.map(new Function<CassandraRow, EventLog>() {
//
//
//			@Override
//			public EventLog call(CassandraRow cassandraRow) throws Exception {
//				EventLog rulesBaseFact = new EventLog();
//				rulesBaseFact.setAssetUid(cassandraRow.getString("asset_uid"));
//				//				String hour = cassandraRow.getString("hour");
//				//				String day = cassandraRow.getString("yyyymmdd");
//				//				String ending_minute = cassandraRow.getString("ending_min");
//				//				cassandraRow.getString("hour");
//
//				rulesBaseFact.setEventType("heatingResponse");
//				//				rulesBaseFact.setAssetUid(cassandraRow.getUUID("log_uuid").toString());
//				Map<Object,Object> measures = cassandraRow.getMap("measures");
//				Map<String, Double> map = new HashMap<String, Double>((Map)measures);
//
//				rulesBaseFact.setMeasures(aggregate(map,new HashMap<String,String>()));
//				return rulesBaseFact;
//			}
//		});
//
//		PairFunction<EventLog, String, EventLog> keyData = new PairFunction<EventLog, String, EventLog>() {
//			public Tuple2<String, EventLog> call(EventLog x) {
//				return new Tuple2(x.getAssetUid(), x); }
//		};
//
//		JavaPairRDD<String, EventLog> eventsByAssets  = rulesFact.mapToPair(keyData);
//
//		JavaPairRDD<String, Iterable<EventLog>> groupedEventsByAssetId =  eventsByAssets.groupByKey();
//
//		JavaRDD<RulesBaseFact> factRdd = groupedEventsByAssetId.map(new Function<Tuple2<String,Iterable<EventLog>>, RulesBaseFact>() {
//
//			@Override
//			public RulesBaseFact call(Tuple2<String, Iterable<EventLog>> events) throws Exception {
//				RulesBaseFact returnVal = new RulesBaseFact();
//				List<AHUBean> ahuMeasures = new ArrayList<>();
//				for(EventLog event : events._2()) {
//					AHUBean ahuBean = new AHUBean();
//					ahuBean.setDeadBand(2d);
//					ahuBean.setITD(3d);
//					ahuBean.setZoneAirTemperatureSP(100d);
//					ahuBean.setZoneAirTemperatureSensor(event.getMeasures().get("zoneAirTempSensor"));
//					ahuMeasures.add(ahuBean);
//				}
//				returnVal.setAhuList(ahuMeasures);
//				return null;
//			}
//		});
//
//		for (RulesBaseFact rulesBaseFact: factRdd.collect()) {
//			LOG.info("AssetId : " + rulesBaseFact.getAssetId() );
//			rulesBaseFact.getAhuList().forEach((v)-> LOG.info(("" + "v")));
//		}
//
//		//		Function2<String, EventLog,Object> reduceByKey = new Function2<String, EventLog, Object>() {
//		//			public Object call(String assetId, EventLog eventLog) {
//		//				return null;
//		//			}
//		//		};
//
//		//eventsByAssets.reduceByKey(func)
//		//		JavaRDD<EventLog> rulesFact = testInputTableRdd.map(new Function<CassandraRow, EventLog>() {
//		//
//		//
//		//			@Override
//		//			public EventLog call(CassandraRow cassandraRow) throws Exception {
//		//				EventLog rulesBaseFact = new EventLog();
//		//				rulesBaseFact.setAssetUid(cassandraRow.getString("asset_uid"));
//		//				String hour = cassandraRow.getString("hour");
//		//				String day = cassandraRow.getString("yyyymmdd");
//		//				String ending_minute = cassandraRow.getString("ending_min");
//		//				cassandraRow.getString("hour");
//		//
//		//				rulesBaseFact.setEventType("heatingResponse");
//		//				rulesBaseFact.setAssetUid(cassandraRow.getUUID("log_uuid").toString());
//		//				Map<Object,Object> measures = cassandraRow.getMap("measures");
//		//				Map<String, Double> map = new HashMap<String, Double>((Map)measures);
//		//				aggregate(map,new HashMap<String,String>());
//		//				rulesBaseFact.setMeasures(map);
//		//				return rulesBaseFact;
//		//			}
//		//		});
//
//
//		Collection<EventLog> rulesBaseFacts = rulesFact.collect();
//		//
//		//		for (EventLog rulesBaseFact: rulesBaseFacts) {
//		//			LOG.info("AssetId : " + rulesBaseFact.getAssetUid() + ", LogUid: " + rulesBaseFact.getLogUUID());
//		//			rulesBaseFact.getMeasures().forEach((k,v)-> LOG.info((k+", "+v)));
//		//		}
//
//		//		rulesFact.mapPartitions(this::processPartition).first();
//
//
//	}
//
//	private Map<String,Double> aggregate(Map<String,Double> measures, Map<String,String> tags) {
//
//		Map<String, Double> returnVal = new HashMap<>();
//		returnVal.put("zoneAirTempSensor", measures.get("zoneAirTempSensor"));
//		return returnVal;
//
//	}
//
//
//	@Override
//	public PropertiesBean getPropertiesBean() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//
//	@Override
//	public String getMaster() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//
//	@Override
//	public String getAppName() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//}