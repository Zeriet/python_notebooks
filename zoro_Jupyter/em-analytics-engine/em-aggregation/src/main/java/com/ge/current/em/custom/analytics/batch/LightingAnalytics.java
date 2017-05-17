package com.ge.current.em.custom.analytics.batch;///*
// * Copyright (c) 2016 GE. All Rights Reserved.
// * GE Confidential: Restricted Internal Distribution
// */
//package com.ge.current.em.custom.analytics.batch;
//
//
//import java.io.Serializable;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.TimeZone;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.PropertySource;
//import org.springframework.core.env.Environment;
//import org.springframework.stereotype.Component;
//
//import com.datastax.spark.connector.japi.CassandraJavaUtil;
//import com.datastax.spark.connector.japi.CassandraRow;
//import com.ge.current.em.entities.analytics.RulesBaseFact;
//import com.ge.current.ie.analytics.ISparkStream;
//import com.ge.current.ie.analytics.rules.IERulesUtil;
//import com.ge.current.ie.bean.EventLogStatus;
//import com.ge.current.ie.bean.PropertiesBean;
//import com.ge.current.ie.model.nosql.EventLog;
//
//
///*
// *SparkMQStream receives messages from MQTT stream and performs business logic on the received messages and
// *stores them into cassandra. There is no aggregation logic happening in this class.
// */
//@Component
//
//@PropertySource(value = "classpath:/application-${spring.profiles.active}.properties")
//public class LightingAnalytics implements Serializable, ISparkStream {
//
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;
//
//	private static final Logger LOG = LoggerFactory.getLogger(LightingAnalytics.class);
//
//	@Autowired
//	private PropertiesBean propertiesBean;
//
//	@Value("${spring.profiles.active}")
//	private String profile;
//
//	@Override
//	public void run(Environment environment, Boolean useSsl, String MQprotocol) {
//		LOG.info("SparkMQStream run method");
//		LOG.info("In SparkMQStream instance: master: {}, appName: {}, cassandraHostName: {}, spark batchInterval: {} ",
//				propertiesBean.getMaster(), propertiesBean.getAppName(), propertiesBean.getCassandraHost(),
//				propertiesBean.getBatchInterval());
//
//		SparkConf sparkConf = new SparkConf().setMaster(propertiesBean.getMaster())
//				.setAppName(propertiesBean.getAppName());
//		sparkConf.set("cassandra.connection.host", propertiesBean.getCassandraHost());
//		sparkConf.set("spark.streaming.unpersist", "false");
//		sparkConf.set("spark.driver.allowMultipleContexts", "true");
//
//		LOG.info("Using Properties ... " + propertiesBean.toString());
//		JavaSparkContext sc = new JavaSparkContext(sparkConf);
//
//		JavaRDD<CassandraRow> testInputTableRdd = CassandraJavaUtil.javaFunctions(sc).cassandraTable(propertiesBean.getKeySpace(), propertiesBean.getCassandraTableName()).limit(10l);
//
//		JavaRDD<EventLog> rulesFact = testInputTableRdd.map(new Function<CassandraRow, EventLog>() {
//
//
//			@Override
//			public EventLog call(CassandraRow cassandraRow) throws Exception {
//				EventLog rulesBaseFact = new EventLog();
//				rulesBaseFact.setAssetUid(cassandraRow.getString("asset_uid"));
//				rulesBaseFact.setEventType("lightingRule");
//				rulesBaseFact.setAssetUid(cassandraRow.getUUID("log_uuid").toString());
//				Map<Object,Object> measures = cassandraRow.getMap("measures");
//				Map<String, Double> map = new HashMap<String, Double>((Map)measures);
//				aggregate(map,new HashMap<String,String>());
//				rulesBaseFact.setMeasures(map);
//				return rulesBaseFact;
//			}
//		});
//
//
//				Collection<EventLog> rulesBaseFacts = rulesFact.collect();
//				
//				for (EventLog rulesBaseFact: rulesBaseFacts) {
//					LOG.info("AssetId : " + rulesBaseFact.getAssetUid() + ", LogUid: " + rulesBaseFact.getLogUUID());
//					 rulesBaseFact.getMeasures().forEach((k,v)-> LOG.info((k+", "+v)));
//				}
//
//		rulesFact.mapPartitions(this::processPartition).first();
//
//	}
//
//	private void aggregate(Map<String,Double> measures, Map<String,String> tags) {
//
//	}
//
//	public Iterable<EventLog> processPartition(Iterator<EventLog> events) {
//		List<EventLog> processedEvents = new ArrayList<>();
//
//		if(propertiesBean.isRulesEnabled()) {
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
//			System.out.println("Rule condition met for asset_id: " + eventProcessingStatus.getEventLog().getAssetUid() + " ,logUUid: " +eventProcessingStatus.getEventLog().getLogUUID() + ", measure: " + eventProcessingStatus.getEventLog().getMeasures().get("zoneAirTempSensor") + " ,conditionMet: "   + eventProcessingStatus.isConditionMet());
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
//		if(propertiesBean.isRulesEnabled()) {
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
//}
