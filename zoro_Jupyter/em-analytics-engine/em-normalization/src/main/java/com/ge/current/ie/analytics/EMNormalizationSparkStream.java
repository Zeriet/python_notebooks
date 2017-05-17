package com.ge.current.ie.analytics;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ge.current.em.aggregation.broadcast.BroadcastManager;
import com.ge.current.em.aggregation.mappers.MessageLogMapper;
import com.ge.current.em.aggregation.request.EventContainer;
import com.ge.current.em.analytics.exception.UnknownTopicException;
import com.ge.current.ie.model.nosql.MessageLog;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.ge.current.em.aggregation.mappers.EventsMapper;
import com.ge.current.em.aggregation.mappers.EmMappers;
import com.ge.current.em.aggregation.dao.OutOfSyncLog;
import com.ge.current.em.aggregation.parser.JACEMessageParser;
import com.ge.current.em.aggregation.parser.WacMessageParser;
import com.ge.current.em.aggregation.utils.JaceUtil;
import com.ge.current.em.aggregation.utils.OutOfSyncUtil;
import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.em.analytics.common.EmailService;
import com.ge.current.em.analytics.dto.JaceEvent;
import com.ge.current.em.analytics.dto.PointObject;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.model.nosql.EventLog;
import com.ge.current.ie.model.nosql.NormalizedEventLog;

import static com.ge.current.em.aggregation.broadcast.BroadcastManager.BroadcastManagerBuilder.aBroadcastManager;
import static com.ge.current.em.aggregation.request.EventContainer.EventContainerBuilder.anEventContainer;

/**
 * Created by 212582112 on 2/16/17.
 */
@Component
@PropertySource(value = "classpath:/application-${spring.profiles.active}.properties")
public class EMNormalizationSparkStream implements ISparkStream, Serializable {
    @Autowired
    PropertiesBean propertiesBean;

    @Autowired
    transient APMDataLoader apmDataLoader;

    @Autowired
    transient EmailService emailService;

    transient private Environment env;
    transient private ScheduledExecutorService scheduledThreadPoolExecutor = Executors.newScheduledThreadPool(1);
    transient private BroadcastManager broadcastManager;

    @Value("${spring.profiles.active}")
    private String profile;

    private SQLContext sqlContext;

    private Broadcast<Map<String, String>> gatewaySiteMapping = null;
    private Broadcast<Map<String, String>> siteTimezoneMapping = null;
    private Broadcast<Map<String, Map<String, PointObject>>> siteHaystackMapping = null;
    private Broadcast<Map<String, String>> assetSiteMapping = null;
    private Broadcast<Map<String, String>> assetEnterpriseMapping = null;
    private Broadcast<Map<String, String>> assetTypeMapping = null;
    private Broadcast<Map<String, String>> assetTimezoneMapping = null;
    private Broadcast<Map<String, Set<String>>> assetSegmentsMapping = null;
    private Broadcast<Map<String, String>> segmentLoadtypeMapping = null;
    private Broadcast<Map<String, String>> resourceNameMapping = null;

    @Override
    public void processStream(JavaDStream<CustomMessage> messageReceiverStream) {
        // @formatter: off
        JACEMessageParser jaceMessageParser = new JACEMessageParser();
        WacMessageParser wacMessageParser = new WacMessageParser();
        JaceUtil jaceUtil = new JaceUtil();

        String jaceTopic = env.getProperty("queues.topic.jace");
        //String jaceTopic = "jace/event";
        String wacTopic = env.getProperty("queues.topic.wac");
        System.out.println("WAC Topic:" + wacTopic);
        // Generate JaceEvents object from the raw messages
        // sent by Jace.
        JavaDStream<EventContainer> jaceEvents = messageReceiverStream.flatMap((CustomMessage message) -> {
            String topic = message.getTopic();
            LOG.error("**** Got Message Topic = " + topic);
            byte[] payload = message.getPayload();

            List<EventContainer> parsedEvents = null;

            try {
                if (topic.equals(jaceTopic) || topic.contains("jace")) {
                    LOG.error("This is a JACE message.");
                    String jaceMessage = new String(payload);
                    System.out.println("*** JACE Message = " + jaceMessage);
                    parsedEvents =  jaceMessageParser.parseMessage(payload, propertiesBean, jaceUtil, gatewaySiteMapping, siteHaystackMapping, siteTimezoneMapping);
                    LOG.error("Done parsing the messages");
                } else if (topic.equals(wacTopic)) {
                    LOG.error("This is a WAC message.");
                    String wacMessage = new String(payload);
                    System.out.println("*** WAC Message = " + wacMessage);
                    parsedEvents = wacMessageParser.parseMessage(payload, propertiesBean, jaceUtil, gatewaySiteMapping, siteHaystackMapping, siteTimezoneMapping);
                } else {
                    String errorMsg = "Cannot parse message from " + topic
                            +"  WAC topic = " + wacTopic;
                    LOG.error(errorMsg);
                    throw new UnknownTopicException(errorMsg);
                }
            }catch (Exception e) {
                LOG.error("Failed to parse message", e);
                parsedEvents = Collections.singletonList(anEventContainer()
                                                            .withInvalidEventStatus()
                                                            .withReasonPhrase(ExceptionUtils.getStackTrace(e))
                                                            .withRawEvent(new String(message.getPayload()))
                                                            .build());
            }

            return parsedEvents;
        }).cache();

        JavaDStream<MessageLog> invalidEvents = jaceEvents.filter(eventContainer -> !eventContainer.isValid())
                                                          .map(MessageLogMapper.generateMessageLog);

        // Convert JaceEvent to EventLog.
        EventsMapper eventsMapper = new EventsMapper();
        JavaDStream<EventLog> events = jaceEvents.filter(jaceEventContainer -> jaceEventContainer.isValid())
                                                 .map(jaceEventContainer -> eventsMapper.generateEventLog(jaceEventContainer.getJaceEvent(), assetEnterpriseMapping.value(), assetSiteMapping.value()))
                                                 .filter(eventLog -> eventLog != null);

        // Convert EventLog to Normalized Event Log.
        JavaDStream<NormalizedEventLog> normalizedEvents = jaceEvents.map(jaceEventContainer -> eventsMapper.normalizeEvent(jaceEventContainer.getJaceEvent(),
                                                                                                                            (assetEnterpriseMapping == null)? new HashMap<>() : assetEnterpriseMapping.value(),
                                                                                                                            (assetSiteMapping == null)? new HashMap<>(): assetSiteMapping.value(),
                                                                                                                            (assetTypeMapping == null)? new HashMap<>() : assetTypeMapping.value(),
                                                                                                                            (assetTimezoneMapping == null)? new HashMap<>(): assetTimezoneMapping.value(),
                                                                                                                            (assetSegmentsMapping == null)? new HashMap<>() : assetSegmentsMapping.value(),
                                                                                                                            (segmentLoadtypeMapping == null)? new HashMap<>() : segmentLoadtypeMapping.value(),
                                                                                                                            (resourceNameMapping == null)? new HashMap<>() : resourceNameMapping.value()))
                                                                    .filter(normalizedEventLog -> normalizedEventLog != null)
                                                                    .cache();

        // Decide if it's out of sync events.
        JavaDStream<OutOfSyncLog> outOfSyncLogs = normalizedEvents.filter(OutOfSyncUtil.filterOutOfSyncNormalizedEvents)
                                                                  .map(EmMappers.mapNormalizedEventLogToOutOfSyncLog);

        // Write to cassandra
        writeToCassandra(events,
                env.getProperty("cassandra.keyspace"),
                env.getProperty("cassandra.table.event_log"),
                EventLog.class);

        writeToCassandra(normalizedEvents,
                env.getProperty("cassandra.keyspace"),
                env.getProperty("cassandra.table.event_norm_log"),
                NormalizedEventLog.class);

        writeToCassandra(outOfSyncLogs,
                env.getProperty("cassandra.keyspace"),
                env.getProperty("cassandra.table.out_of_sync_log"),
                OutOfSyncLog.class);

        writeToCassandra(invalidEvents,
                env.getProperty("cassandra.keyspace"),
                env.getProperty("cassandra.table.error_log"),
                MessageLog.class);
        // @formatter: on
    }

    @Override
    public void broadcast(JavaStreamingContext jsc, Environment environment) {
        this.env = environment;
        this.broadcastManager = aBroadcastManager()
                                    .withJavaStreamingContext(jsc)
                                    .build();

        // To make sure that the broadcast variables are
        // initialized when the job starts
        refreshAllBroadcast(jsc);

        // Refresh after normalization job start
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(() -> refreshAllBroadcast(jsc),
                                                           env.getProperty("broadcast.refresh.intervalInMins", Long.class, 60L),
                                                           env.getProperty("broadcast.refresh.intervalInMins", Long.class, 60L),
                                                           TimeUnit.MINUTES);
    }

    public void refreshAllBroadcast(JavaStreamingContext jsc) {
        System.out.println("Refreshing broadcast at " + new Date());

        try {
            apmDataLoader.generateMappings();

            gatewaySiteMapping = broadcastManager.refreshBroadcast(gatewaySiteMapping, new HashMap<>(), () -> apmDataLoader.getGatewaySiteMapping());
            siteHaystackMapping = broadcastManager.refreshBroadcast(siteHaystackMapping, new HashMap<>(), () -> apmDataLoader.getSiteHaystackMapping());
            siteTimezoneMapping = broadcastManager.refreshBroadcast(siteTimezoneMapping, new HashMap<>(), () -> apmDataLoader.generateSiteTimeZoneMapping());
            assetSiteMapping = broadcastManager.refreshBroadcast(assetSiteMapping, new HashMap<>(), () -> apmDataLoader.generateAssetSiteMapping());
            assetEnterpriseMapping = broadcastManager.refreshBroadcast(assetEnterpriseMapping, new HashMap<>(), () -> apmDataLoader.generateAssetEnterpriseMapping());
            assetTypeMapping = broadcastManager.refreshBroadcast(assetTypeMapping, new HashMap<>(), () -> apmDataLoader.generateAssetTypeMapping());
            assetTimezoneMapping = broadcastManager.refreshBroadcast(assetTimezoneMapping, new HashMap<>(), () -> apmDataLoader.generateAssetTimezoneMapping());
            assetSegmentsMapping = broadcastManager.refreshBroadcast(assetSegmentsMapping, new HashMap<>(), () -> apmDataLoader.generateAssetSegmentMapping());
            segmentLoadtypeMapping = broadcastManager.refreshBroadcast(segmentLoadtypeMapping, new HashMap<>(), () -> apmDataLoader.getLoadTypeMapping());
            resourceNameMapping = broadcastManager.refreshBroadcast(resourceNam  eMapping, new HashMap<>(), () -> apmDataLoader.getResourceNameMapping());

        } catch (Exception e) {
        	System.err.println("Error loading data:" + e.getMessage());
        	e.printStackTrace();
        	
        	try {
        		// TODO: Send an email with the error details
                Map<String, String> configErrorMap = apmDataLoader.getConfigErrorMap();
                emailService.sendMessage("EM Normalization Spark Job",
                           jsc.sparkContext().sc().applicationId(), configErrorMap, e,
                           "EM Normalization Spark Job broadcast Alert.");
        	} catch (Exception ex) {
        		System.err.print("Error sending email:" + ex.getMessage());
        	}
            
                       
        }

    }

    @Override
    public PropertiesBean getPropertiesBean() {
        return propertiesBean;
    }

    @Override
    public String getActiveProfile() {
        return profile;
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
}
