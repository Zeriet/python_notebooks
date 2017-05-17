package com.ge.current.ie.analytics;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.ge.current.ie.analytics.BroadcastManager.BroadcastManagerBuilder.aBroadcastManagerBuilder;
import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_CONNECTION_HOST;
import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_CONNECTION_PORT;
import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_USERNAME;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.em.edgealarms.parser.JaceAlarmMessageParser;
import com.ge.current.em.edgealarms.parser.WacAlarmMessageParser;
import com.ge.current.em.entities.analytics.AlertLog;
import com.ge.current.em.entities.analytics.AlertLogStg;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;

/**
 * Created by 212582112 on 2/16/17.
 */
@Component
@PropertySource(value = "classpath:/application-${spring.profiles.active}.properties")
public class EMEdgeAlarmsSparkStream implements ISparkStream, Serializable {
	@Autowired
	PropertiesBean propertiesBean;

	@Autowired
	transient APMDataLoader apmDataLoader;
	//transient ApmCacheService apmDataLoader = new JsonApmCacheServiceImpl("apm-cache.json");
	
	transient private Environment env;
	transient private ScheduledExecutorService scheduledThreadPoolExecutor = Executors.newScheduledThreadPool(1);
	transient private BroadcastManager broadcastManager;
    transient private DbUtils dbUtils;

	@Value("${spring.profiles.active}")
	private String profile;

	private Broadcast<Map<String, String>> gatewaySiteMapping = null;
	private Broadcast<Map<String, String>> siteTimezoneMapping = null;
	private Broadcast<Map<String, String>> resourceNameMapping = null;
	private Broadcast<Map<String, String>> siteEnterpriseMapping = null;

	@Override
	public void processStream(JavaDStream<CustomMessage> messageReceiverStream) {
		// @formatter: off
		String jaceTopic = env.getProperty("queues.topic.jace");
		// String jaceTopic = "jace/event";
		String wacTopic = env.getProperty("queues.topic.wac");
		
		final DbProperties dbProp = new DbProperties()
				.setAlarmBuckets(Integer.valueOf(env.getProperty("cassandra.stag.buckets")))
				.setContactPoints(propertiesBean.getValue(CASSANDRA_CONNECTION_HOST))
				.setKeyspace(env.getProperty("cassandra.keyspace"))
				.setPassword(propertiesBean.getValue(SparkConfigurations.CASSANDRA_PASSWORD))
				.setPort(Integer.valueOf(propertiesBean.getValue(CASSANDRA_CONNECTION_PORT)))
				.setStagingTable(env.getProperty("cassandra.table.name.stg"))
				.setUser(propertiesBean.getValue(CASSANDRA_USERNAME));
		
		System.out.println("DbProperties:" + ReflectionToStringBuilder.toString(dbProp));
		System.out.println("JACE Topic:" + jaceTopic);
		System.out.println("WAC Topic:" + wacTopic);
		
		WacAlarmMessageParser wacParser = new WacAlarmMessageParser();
		JaceAlarmMessageParser jaceParser = new JaceAlarmMessageParser();
		
		try {
			JavaDStream<AlertLog> edgeAlarms = messageReceiverStream.flatMap((CustomMessage message) -> {
				String topic = message.getTopic();
				System.out.println("**** Got Message Topic = " + topic);
				byte[] payload = message.getPayload();
				if (topic.equals(wacTopic)) {
					System.out.println("This is a WAC message.");
					String wacMessage = new String(payload);
					System.out.println("*** WAC Message = " + wacMessage);
					return wacParser.processAlarmData(payload, gatewaySiteMapping, siteTimezoneMapping, 
							resourceNameMapping, siteEnterpriseMapping, dbProp);
				} else if (topic.equals(jaceTopic)) { 
					System.out.println("This is a JACE message.");
					String jaceMessage = new String(payload);
					System.out.println("*** JACE Message = " + jaceMessage);
					return jaceParser.processAlarmData(payload, gatewaySiteMapping, siteTimezoneMapping, 
							resourceNameMapping, siteEnterpriseMapping, dbProp);
				} else {
					System.err.println("Cannot parse message from " + topic);
					return Collections.emptyList();
				}
			}).cache();
			System.out.println("Ready to Save.");
			
			// After saving
			// This is for temporary testing as Spark Node doesn't process 
			JavaDStream<AlertLogStg> stagEvents = edgeAlarms.map(event -> DbUtils.generateAlertLogStag(event))
						.filter(eventLog -> eventLog != null).cache();

			JavaDStream<AlertLogStg> toSave = stagEvents.filter(filterAlert);

			writeToCassandra(toSave,
	                dbProp.getKeyspace(),
	                dbProp.getStagingTable(),
	                AlertLogStg.class);
			
			System.out.println("Saved Events.");
			
		} catch (Exception e) {
			// Any exceptions should be persisted into cassandra error table.
			LOG.error("Failed to parse message. Error: ", e);
		}
		// @formatter: on
	}

	@Override
	public void broadcast(JavaStreamingContext jsc, Environment environment) {
		this.env = environment;
		this.broadcastManager = aBroadcastManagerBuilder().withJavaStreamingContext(jsc).build();

		// To make sure that the broadcast variables are
		// initialized when the job starts
		refreshAllBroadcast(jsc);

		// Refresh after normalization job start
		scheduledThreadPoolExecutor.scheduleWithFixedDelay(() -> refreshAllBroadcast(jsc),
				env.getProperty("broadcast.refresh.intervalInMins", Long.class, 10L),
				env.getProperty("broadcast.refresh.intervalInMins", Long.class, 10L), TimeUnit.MINUTES);
	}

	public void refreshAllBroadcast(JavaStreamingContext jsc) {
		System.out.println("Refreshing broadcast at " + new Date());

		try {
            apmDataLoader.generateMappingForSiteOffline();
			gatewaySiteMapping = broadcastManager.refreshBroadcast(gatewaySiteMapping,
					() -> apmDataLoader.getGatewaySiteMapping());
			resourceNameMapping = broadcastManager.refreshBroadcast(resourceNameMapping,
					() -> apmDataLoader.getResourceNameMapping());
			siteTimezoneMapping = broadcastManager.refreshBroadcast(siteTimezoneMapping,
					() -> apmDataLoader.generateSiteTimeZoneMapping());
			siteEnterpriseMapping = broadcastManager.refreshBroadcast(siteEnterpriseMapping,
					() -> apmDataLoader.generateSiteEnterpriseMapping());
		} catch (Exception e) {
			// TODO: Send an email with the error details
		}
	}
	
	public <T> void writeToCassandra(JavaDStream<T> events, String keyspace, String tableName, Class<T> className) {
        // @formatter: off
        events.foreachRDD(new Function<JavaRDD<T>, Void>() {
            @Override
            public Void call(JavaRDD<T> eventsRDD) throws Exception {
                javaFunctions(eventsRDD).writerBuilder(keyspace,
                                                       tableName,
                                                       mapToRow(className))
                                        .saveToCassandra();

                return null;
            }
        });
        // @formatter: on
    }
	
	public static Function<AlertLogStg, Boolean> filterAlert = new Function<AlertLogStg, Boolean>() {

		@Override
		public Boolean call(AlertLogStg event) throws Exception {
			return false;
		}
	};
	
	@Override
	public PropertiesBean getPropertiesBean() {
		return propertiesBean;
	}

	@Override
	public String getActiveProfile() {
		return profile;
	}
}
