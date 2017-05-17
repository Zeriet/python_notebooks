package com.ge.current.ie.analytics.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_CONNECTION_PORT;
import static com.ge.current.ie.bean.SparkConfigurations.SPARK_APP_NAME;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import java.sql.Timestamp;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.em.analytics.common.EmailService;
import com.ge.current.em.aggregation.dao.EMUsageEvent;
import com.ge.current.em.aggregation.dao.BulkData;
import com.ge.current.em.aggregation.dao.IECommonEvent;
import com.ge.current.em.aggregation.utils.AggregationUtil;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.ie.analytics.batch.ISparkBatch;
import com.ge.current.ie.analytics.batch.service.AggregationService;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;
import com.ge.current.ie.model.nosql.EventsByMinute;
import com.ge.current.ie.model.nosql.EventsByMinuteStaging;

import com.ge.current.ie.queueutil.IEAmqpUtil;
import scala.Tuple2;

@Component
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class MinuteBatch implements ISparkBatch, Serializable {

	private static final Logger logger = LoggerFactory.getLogger(MinuteBatch.class);

	@Autowired
	PropertiesBean propertiesBean;

	@Autowired
	transient APMDataLoader apmDataLoader;

	@Autowired
	transient EmailService emailService;

	private static String KWH_MEASURE = "measureskWh";

	private Broadcast<Map<String,String>> assetSiteMappingBC = null;
    private Broadcast<Map<String,String>> assetEnterpriseMappingBC = null;
	private Broadcast<Map<String, String>> siteEnterpriseMappingBC = null;
	private Broadcast<Map<String, String>> segmentEnterpriseMappingBC = null;
    private Broadcast<Map<String, Set<String>>> assetSegmentMappingBC = null;
    private Broadcast<Map<String, Set<String>>> assetSubmeterMappingBC = null;
    private Broadcast<Map<String, Set<String>>> segmentSubmeterMappingBC = null;
    private Broadcast<Map<String, Set<String>>> siteSubmeterMappingBC = null;
    private Broadcast<Map<String, Set<String>>> enterpriseSubmeterMappingBC = null;
	private Broadcast<Map<String, String>> segmentLoadTypeMappingBC = null;
	private Broadcast<Set<String>> billingListBC = null;

	@Override
	public PropertiesBean getPropertiesBean() {
		return propertiesBean;
	}

	@Override
	public String getAppName() {
		return propertiesBean.getValue(SPARK_APP_NAME) + "_15MinsSummaryBatchJob";
	}

	@Override
	public String getMaster() {
		return null;
	}

	@Override
	public void broadcast(SparkContext sc, PropertiesBean propertiesBean) {
		System.out.println("**** Sending Broadcast assetSegmentMappingBC ****");

        try {
            apmDataLoader.generateMappings();
        } catch (Exception ex) {
            Map<String, String> configErrorMap = apmDataLoader.getConfigErrorMap();
            emailService.sendMessage("EM 15mins Aggregation Spark Job",
                     sc.applicationId(), configErrorMap, ex,
                    "EM 15 mins Spark Job broadcast Alert."); 
        }

        Map<String, String> assetEnterpriseMapping = apmDataLoader.generateAssetEnterpriseMapping();
        Map<String, String> assetSiteMapping = apmDataLoader.generateAssetSiteMapping();
		Map<String, String> siteEnterpriseMapping = apmDataLoader.generateSiteEnterpriseMapping();
		Map<String, String> segmentEnterpriseMapping = apmDataLoader.generateSegmentEnterpriseMapping();
		Map<String, Set<String>> subMeterMapping = apmDataLoader.getSubMeterToSegmentsMapping();
		Map<String, Set<String>> assetSegmentMapping = apmDataLoader.generateAssetSegmentMapping();

		Map<String, Set<String>> assetSubmeterMapping = apmDataLoader.generateAssetSegmentMapping();
		Map<String, Set<String>> segmentSubmeterMapping = apmDataLoader.getSegmentToSubmeterMapping();
		Map<String, Set<String>> siteSubmeterMapping = apmDataLoader.getSiteToSubmeterMapping();
		Map<String, Set<String>> enterpriseSubmeterMapping = apmDataLoader.getEnterpriseToSubmeterMapping();
		Map<String, String> segmentLoadTypeMapping = apmDataLoader.getLoadTypeMapping();

        Set<String> billingList = apmDataLoader.getBillingList();

        assetEnterpriseMappingBC = sc.broadcast(assetEnterpriseMapping, scala.reflect.ClassTag$.MODULE$.apply(assetEnterpriseMapping.getClass()));
        assetSiteMappingBC = sc.broadcast(assetSiteMapping, scala.reflect.ClassTag$.MODULE$.apply(assetSiteMapping.getClass()));
		siteEnterpriseMappingBC = sc.broadcast(siteEnterpriseMapping, scala.reflect.ClassTag$.MODULE$.apply(siteEnterpriseMapping.getClass()));
		segmentEnterpriseMappingBC = sc.broadcast(segmentEnterpriseMapping, scala.reflect.ClassTag$.MODULE$.apply(segmentEnterpriseMapping.getClass()));
        assetSegmentMappingBC = sc.broadcast(assetSegmentMapping, scala.reflect.ClassTag$.MODULE$.apply(assetSegmentMapping.getClass()));

        assetSubmeterMappingBC = sc.broadcast(assetSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(assetSubmeterMapping.getClass()));
		segmentSubmeterMappingBC = sc.broadcast(segmentSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(segmentSubmeterMapping.getClass()));
		siteSubmeterMappingBC = sc.broadcast(siteSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(siteSubmeterMapping.getClass()));
        enterpriseSubmeterMappingBC = sc.broadcast(enterpriseSubmeterMapping, scala.reflect.ClassTag$.MODULE$.apply(enterpriseSubmeterMapping.getClass()));
		segmentLoadTypeMappingBC = sc.broadcast(segmentLoadTypeMapping, scala.reflect.ClassTag$.MODULE$.apply(segmentLoadTypeMapping.getClass()));

		billingListBC = sc.broadcast(billingList, scala.reflect.ClassTag$.MODULE$.apply(billingList.getClass()));
		System.out.println(">>> Segment Loadtype mapping: " + segmentLoadTypeMapping);
		System.out.println(">>> Broadcasted loadtype mapping: " + segmentLoadTypeMappingBC.value());
		System.out.println(">>> Billing List: " + billingListBC.value());
	}
	
	
	public  void setMetaData(Broadcast<Map<String,String>> assetSiteMappingBC,
			Broadcast<Map<String,String>> assetEnterpriseMappingBC,
			Broadcast<Map<String, String>> siteEnterpriseMappingBC,
			Broadcast<Map<String, String>> segmentEnterpriseMappingBC,
			Broadcast<Map<String, Set<String>>> assetSegmentMappingBC,
			Broadcast<Map<String, Set<String>>> assetSubmeterMappingBC,
			Broadcast<Map<String, Set<String>>> segmentSubmeterMappingBC,
			Broadcast<Map<String, Set<String>>> siteSubmeterMappingBC,
			Broadcast<Map<String, Set<String>>> enterpriseSubmeterMappingBC,
			Broadcast<Map<String, String>> segmentLoadTypeMappingBC,
            Broadcast<Set<String>> billingListBC,
			PropertiesBean propertiesBean
			) {
		System.out.println("**** Set MetaData for 15mins ****");
		this.assetEnterpriseMappingBC = assetEnterpriseMappingBC;
		this.assetSiteMappingBC =  assetSiteMappingBC;
		this.siteEnterpriseMappingBC =  siteEnterpriseMappingBC;
		this.segmentEnterpriseMappingBC = segmentEnterpriseMappingBC;
		this.assetSegmentMappingBC = assetSegmentMappingBC;
		this.assetSubmeterMappingBC = assetSubmeterMappingBC;
		this.segmentSubmeterMappingBC = segmentSubmeterMappingBC;
		this.siteSubmeterMappingBC = siteSubmeterMappingBC;
		this.enterpriseSubmeterMappingBC = enterpriseSubmeterMappingBC;
		this.segmentLoadTypeMappingBC = segmentLoadTypeMappingBC;
        this.billingListBC = billingListBC;
		this.propertiesBean = propertiesBean;
	}

	@Override
	public void run(Environment env, String startDate, String endDate, String timeZone) {

		logger.error(".... Scheduler Started for .... " + startDate);

        // @formatter: off
        AggregationService aggregationService = new AggregationService();

        SparkContext sc = aggregationService.getCassandraConfiguredSparkContext(getAppName(), getPropertiesBean());

		CassandraSQLContext cassandraSQLContext = new CassandraSQLContext(sc);

		// broadcast
		broadcast(sc, getPropertiesBean());

		// String pastTimeBucket = "201612150200";
		if (startDate == null) return;

        //String[] timePair = getStartEndTimePair(startDate);

        try {
            JavaRDD<EventsByMinute> allEventsByMinute = 
                    process15MinsForResource(cassandraSQLContext,
                        propertiesBean.getValue(SparkConfigurations.CASSANDRA_KEYSPACE), 
                        startDate);

            // send all the 15mins data to the B&R queue. 
            sendMinEventsToQueue(allEventsByMinute, true);
            System.out.println("**** All Data Sent, 15 Mins Job Done ***");

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
            
		logger.error("****** 15 Mins SCHEDULER ENDED ******");
	}

	public JavaRDD<EventsByMinute> process15MinsForResource(
            CassandraSQLContext cassandraSQLContext,
			String keySpace, String timeBucket) throws Exception {

		String query = "select resrc_uid, measures, tags, site_uid, enterprise_uid, event_ts, event_ts_tz, time_bucket, ext_props from "
				+ keySpace + ".event_norm_log where " + " time_bucket = '" + timeBucket + "' and event_type='reading'";
				//+ " and event_ts < '" + endTs + "' and event_ts >= '" + startTs + "'";

		System.out.println("=== query is " + query);

		DataFrame dataFrame = cassandraSQLContext.cassandraSql(query);

		return aggregate15MinData(keySpace, dataFrame);

	}
	
	
	public void process15MinsForResource(SparkContext sc, CassandraSQLContext cassandraSQLContext,
			String keySpace, List<String> timeBucket) throws Exception {
		String timeBucketStr = String.join(",", timeBucket);
		String query = "select resrc_uid, measures, tags, site_uid, enterprise_uid, event_ts, event_ts_tz, time_bucket, ext_props from "
				+ keySpace + ".event_norm_log where " + " time_bucket IN (" + timeBucketStr + ")" ;
		logger.info("=== query is " + query);
		System.out.println("=== query is " + query);
		DataFrame dataFrame = cassandraSQLContext.cassandraSql(query);
		aggregate15MinData(keySpace, dataFrame);
	}

	private JavaRDD<EventsByMinute> aggregate15MinData(String keySpace, DataFrame dataFrame) {
		Map<String, Set<String>> assetSegmentMapping = 
				(assetSegmentMappingBC == null)? new HashMap<String, Set<String>>() : assetSegmentMappingBC.getValue();
				
		JavaRDD<IECommonEvent> aggregatedEvents = 
				   dataFrame.filter( dataFrame.col("measures").isNotNull().or 
                                    (dataFrame.col("tags").isNotNull()))
                            .javaRDD()
							.map(rowMapperToCommonEvent())
							.flatMap(populateSegmentsAndSubMeter(assetSegmentMapping))
							.mapToPair(commonEvent -> {
								return new Tuple2<String, IECommonEvent>(
                                commonEvent.timeBucket + commonEvent.resourceId, commonEvent);
							})
							.reduceByKey(AggregationUtil.aggregateNormalizedEvents)
							.map(pair ->{ return pair._2; }).cache();

		JavaRDD<EventsByMinuteStaging> minStageEvents = aggregatedEvents.map(commonEventToEventsByMinuteStaging());
		
		javaFunctions(minStageEvents)
		.writerBuilder(keySpace, "events_byminute_stg", mapToRow(EventsByMinuteStaging.class))
		.saveToCassandra();
		
		JavaRDD<EventsByMinute> minEvents =  minStageEvents.map(eventsByMinuteStgToEventsByMinute()).cache();
		
		javaFunctions(minEvents)
		.writerBuilder(keySpace, "events_byminute", mapToRow(EventsByMinute.class))
		.saveToCassandra();

        return minEvents;
    }

    public void sendMinEventsToQueue(JavaRDD<EventsByMinute> minEvents, boolean closeConnection) {
        // sending Billing&Rating events to the queue.
        JavaRDD<EventsByMinute> brEvents = minEvents.filter(event -> {
            // only the load type segment and SITE will be sent over. 
            Set<String> billingList = billingListBC.getValue();
            return billingList.contains(event.getResrc_uid()) || 
                   event.getEvent_bucket().contains("SEGMENT_TYPE");
        });

        List<EventsByMinute> brEventsList = brEvents.collect();
        for(EventsByMinute emin: brEventsList) {
            System.out.println(" BR Events : " + emin.getEvent_bucket());
        }
        List<EMUsageEvent> usageList = new ArrayList();

        if (brEventsList.size() > 0) {
            brEventsList.forEach(brEvent -> { 
                eventUsageMapper(brEvent, usageList);
            });

            System.out.println("**** group the Usage data, list size = " + usageList.size());
            BulkData bdata = new BulkData();
            bdata.setEmUsageEvents(usageList);
            bdata.setDataSourceType("MEASURED");
            
            IEAmqpUtil.publishMessage(propertiesBean, bdata);
            System.out.println("Successfully sent " + bdata.getEmUsageEvents().size() 
                    + " events to queue.");

            // close the channel
            if (closeConnection) {
                try {
                    System.out.println("***** Now Closing Connection ****");
                    IEAmqpUtil.closeConnection();
                    System.out.println("***** Connection Closed ****");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
	}

	private Function<EventsByMinuteStaging, EventsByMinute> eventsByMinuteStgToEventsByMinute() {
		return  eMinStg-> {
			EventsByMinute eventsByMin = new EventsByMinute();
			try {
				BeanUtils.copyProperties(eventsByMin, eMinStg);
			} catch (Exception e) {
				System.out.println("Error while copying beans from Events15MinsStg to EventsByMinute");
				e.printStackTrace();
			}

            String resourceType = eMinStg.getResrc_type();
            String resourceUid = eMinStg.getResrc_uid();
            String timeZone = eMinStg.getEvent_bucket();

            if(resourceType.equalsIgnoreCase("SEGMENT") &&
                   segmentLoadTypeMappingBC != null &&
                   segmentLoadTypeMappingBC.value().containsKey(resourceUid)) {
                String loadTypeUid = segmentLoadTypeMappingBC.value().get(resourceUid).split(":")[0];
                System.out.println("**** loadTypeUid = " + loadTypeUid);

                eventsByMin.setEvent_bucket(timeZone + "." + resourceType + "." 
                        + loadTypeUid + "." + resourceUid + ".reading");
            } else {
                eventsByMin.setEvent_bucket(timeZone + "." + resourceType + "."
                            + resourceUid + ".reading");
            }
			return eventsByMin;
		};
	}

	private Function<IECommonEvent, EventsByMinuteStaging> commonEventToEventsByMinuteStaging() {
		return commonEvent -> {

			String yyyymmddhh = commonEvent.timeBucket.substring(0, 10);
			String start_min = commonEvent.timeBucket.substring(10);
			//logger.error(" *** yyyymmdd = " + yyyymmddhh + " hhmm = " + start_min);
			Date utcDate = DateUtils.convertToUTC(yyyymmddhh + start_min, "yyyyMMddHHmm");
			String localTimeString = DateUtils.convertUTCDateToLocalString(utcDate, commonEvent.timeZone, "yyyyMMddHHmm");

            System.out.println("**** EventsByMinuteStg: resrc_uid = " + commonEvent.resourceId);
			
			return new EventsByMinuteStaging(commonEvent.timeZone, 
						localTimeString.substring(0, 8), 
						localTimeString.substring(8),
						commonEvent.resourceType, 
						commonEvent.resourceId, 
						15, UUIDs.timeBased(), 
						utcDate,
						commonEvent.enterpriseId, "", "", "", "", "", null, null, null,
					    getPrefixMap(commonEvent.measures_aggr, "measures_aggr"),
					    getPrefixMap(commonEvent.measures_cnt, "measures_cnt"),
					    getPrefixMap(commonEvent.measures_min, "measures_min"),
					    getPrefixMap(commonEvent.measures_max, "measures_max"),
					    getPrefixMap(commonEvent.measures_avg, "measures_avg"),
                        getPrefixTagMap(commonEvent.tags, "tags"),
                        commonEvent.ext_props);
                        //getPrefixTagMap(commonEvent.ext_props, "ext_props"));
		};
	}

    // this is the function to populate all the events list for assets/segments/sites/ent.
	private  FlatMapFunction<IECommonEvent, IECommonEvent> populateSegmentsAndSubMeter(
			Map<String, Set<String>> assetSegmentMapping) {
		return asset -> {
			
			List<IECommonEvent> list = new ArrayList<>();
			if(asset != null){
				list.add(asset);
				// Add Segments Copy
				if (assetSegmentMapping != null && 
                        assetSegmentMapping.containsKey(asset.resourceId)) {
					Set<String> segments = assetSegmentMapping.get(asset.resourceId);

					segments.stream().forEach(segmentUid -> {
						IECommonEvent segmentCopy = asset.clone();
                        segmentCopy.resourceId = segmentUid;
                        segmentCopy.resourceType = "SEGMENT";
                        removeSubmeter(asset, segmentCopy);
                        list.add(segmentCopy);
					});
				}

				// Add Site Copy
				IECommonEvent siteCopy = asset.clone();
                siteCopy.resourceId = asset.siteId;
                siteCopy.resourceType = "SITE";
                removeSubmeter(asset, siteCopy);
                list.add(siteCopy);

				//Add Enterprise Copy
				IECommonEvent entCopy = asset.clone();
                entCopy.resourceId = asset.enterpriseId;
                entCopy.resourceType = "ENTERPRISE";
                removeSubmeter(asset, entCopy);
                list.add(entCopy);
			}
			return list;
		};
	}

    // this function is to keep the kWh in the correlated sites/segments/enterprise, 
    // but take out kWh value for all other unrelated resources. 
	public void removeSubmeter(IECommonEvent asset, IECommonEvent siteCopy) {
		Set<String> submeterList = getResourceSubmeterMapping(siteCopy.resourceId, 
                siteCopy.resourceType);
		if(submeterList != null && !submeterList.contains(asset.resourceId)) {
			//logger.error("Removing kWH for resourceUid: " + siteCopy.resourceId + " of type: " + siteCopy.resourceType);
		    // If this is not a submeter then remove the kWh value
			siteCopy.measures_aggr.remove(KWH_MEASURE);
			siteCopy.measures_max.remove(KWH_MEASURE);
		    siteCopy.measures_min.remove(KWH_MEASURE);
		    siteCopy.measures_cnt.remove(KWH_MEASURE);
		    siteCopy.measures_aggr.remove(KWH_MEASURE);
		}
	}

	public static Function<Row, IECommonEvent> rowMapperToCommonEvent() {
		return row -> {
            Map<String, Double> measures = null;
            Map<String, String> tags = null;

            if (! row.isNullAt(1)) {
                measures = new HashMap(row.getJavaMap(1));
            } else {
               measures = new HashMap(); 
            }

            if (! row.isNullAt(2)) {
                tags = new HashMap(row.getJavaMap(2));
            } 
            
            IECommonEvent commonEvent = new IECommonEvent(measures);
            commonEvent.resourceId = row.getString(0);
            commonEvent.tags =tags;
            commonEvent.siteId = row.getString(3);
            commonEvent.enterpriseId = row.getString(4);
            commonEvent.resourceType = "ASSET";

            if ( !row.isNullAt(5)) {
                System.out.println("==== Date = " + row.get(5).toString());
                SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
                String timeString = row.get(5).toString();

                commonEvent.eventTs = sdf.parse(timeString); 
            }

            commonEvent.timeZone = row.getString(6);
            System.out.println("==== Timezone = " + commonEvent.timeZone);

            commonEvent.timeBucket = row.getString(7);
            commonEvent.ext_props = row.isNullAt(8)? new HashMap<>(): new HashMap(row.getJavaMap(8));

            System.out.println("==== Timebucket = " + commonEvent.timeBucket);
            return commonEvent;
		};
	}

    private void eventUsageMapper(EventsByMinute event15mins, List<EMUsageEvent> usageList)
    {
        // if no kWh data, just ignore it. 
        if (event15mins.getMeasures_aggr() == null ||
            event15mins.getMeasures_aggr().get("measures_aggrkWh") == null) {
            return;
        }

        // only send the site/segment with kwh values.
        EMUsageEvent usage = new EMUsageEvent();
        String resourceUid = event15mins.getResrc_uid();

        usage.setEntitySourceKey(resourceUid);
        usage.setGtSourceKey(resourceUid);
        usage.setUsageUOM("KILOWATTS_PER_HOUR");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        String timezone = event15mins.getEvent_bucket().split("\\.")[0];
        sdf.setTimeZone(TimeZone.getTimeZone(timezone));
        Date startDate = event15mins.getEvent_ts();
        String startTime = sdf.format(startDate);
        usage.setStartTime(startTime);

        System.out.println("SEGMENT ID = " + resourceUid + " timezone= " + timezone
            + " start time = " + startTime
            + " kWh = " +
               event15mins.getMeasures_aggr().get("measures_aggrkWh"));

        usage.setUsageAmount(event15mins.getMeasures_aggr().get("measures_aggrkWh")   );;
        // 15 mins = 900 seconds.
        usage.setDuration(900);

        usageList.add(usage);
    }


	public Set<String> getResourceSubmeterMapping(String resourceUid, String resourceType) {
		Map<String, Set<String>> resourceSubmeterMapping = null;
	    switch(resourceType) {
            case "SEGMENT": resourceSubmeterMapping = (segmentSubmeterMappingBC == null)? new HashMap<>() : segmentSubmeterMappingBC.getValue(); break;
            case "SITE": resourceSubmeterMapping = (siteSubmeterMappingBC == null)? new HashMap<>() : siteSubmeterMappingBC.getValue(); break;
            case "ENTERPRISE": resourceSubmeterMapping = (enterpriseSubmeterMappingBC == null)? new HashMap<>() : enterpriseSubmeterMappingBC.getValue(); break;
        }

        if(resourceSubmeterMapping != null && resourceSubmeterMapping.containsKey(resourceUid)) {
            return resourceSubmeterMapping.get(resourceUid);
        }

       return new HashSet<>();
    }

    public Map<String, Double> getPrefixMap(Map<String, Double> origMap, String prefix)
    {
        Map<String, Double> prefixMap = new HashMap();
        if (origMap != null) {
            for(String key: origMap.keySet()) {
                if (key.startsWith("measurestags")) {
                    prefixMap.put(key.replaceFirst("measurestags", prefix), origMap.get(key));
                } else {
                    prefixMap.put(key.replaceFirst("measures", prefix), origMap.get(key));
                }
            }
        }
        return prefixMap;
    }

    public Map<String, String> getPrefixTagMap(Map<String, String> origMap, String prefix)
    {
        Map<String, String> prefixMap = new HashMap();
        if (origMap != null) {
            for(String key: origMap.keySet()) {
                System.out.println("*** Tag Key = " + key + " value = " + origMap.get(key));
                prefixMap.put(key.replaceFirst("measures", prefix), origMap.get(key));
            }
        }
        return prefixMap;
    }
	
	private String[] getStartEndTimePair(String timeString) {
		Date startDate = null;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		try {
			startDate = sdf.parse(timeString);
		} catch (Exception e) {
			e.printStackTrace();
		}

		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Calendar cal = Calendar.getInstance();
		cal.setTime(startDate);
		String startTimeString = sdf.format(startDate);

		cal.add(Calendar.MINUTE, 15);
		Date endDate = cal.getTime();
		String endTimeString = sdf.format(endDate);
		String[] timePair = { startTimeString, endTimeString };
		logger.error("startTimeString = " + timePair[0] + " endTimeString=" + endTimeString);
		return timePair;
	}
}
