package com.ge.current.ie.analytics.batch;

import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_CONNECTION_PORT;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import com.ge.current.ie.queueutil.IEAmqpUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.ge.current.em.analytics.common.APMDataLoader;
import com.ge.current.em.analytics.common.EmailService;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.em.aggregation.dao.OutOfSyncLog;
import com.ge.current.ie.analytics.batch.MinuteBatch;
import com.ge.current.ie.analytics.batch.service.AggregationService;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;
import com.ge.current.ie.model.nosql.EventsByMinute;

import scala.Tuple2;


@Component
@PropertySource("classpath:application-${spring.profiles.active}.properties")
public class OutOfSyncBatchStg implements ISparkBatch, Serializable {

	@Autowired
	PropertiesBean propertiesBean;

	@Autowired
	transient APMDataLoader apmDataLoader;

	@Autowired
	transient EmailService emailService;

    @Autowired
    SparkCassandraKPITillDateAggregation kpiCalculation;

	Broadcast<Map<String, Set<String>>> assetSegmentMappingBC = null;
	Broadcast<Map<String, String>> assetSiteMappingBC = null;
	Broadcast<Map<String, String>> assetEnterpriseMappingBC = null;
	Broadcast<Map<String, String>> assetTypeMappingBC = null;
	private static Broadcast<Map<String, String>> siteEnterpriseMappingBC = null;
	private static Broadcast<Map<String, String>> segmentEnterpriseMappingBC = null;
	private static Broadcast<Map<String, List<String>>> subMeterToSegmentsMappingBC = null;

	private Broadcast<Map<String, Set<String>>> assetSubmeterMappingBC = null;
	private Broadcast<Map<String, Set<String>>> segmentSubmeterMappingBC = null;
	private Broadcast<Map<String, Set<String>>> siteSubmeterMappingBC = null;
	private Broadcast<Map<String, Set<String>>> enterpriseSubmeterMappingBC = null;
	private Broadcast<Map<String, String>> segmentLoadTypeMappingBC = null;
    private Broadcast<Set<String>> billingListBC = null;

    // KPI Job broadcast variables
    private Broadcast<Map<String, HashMap<String, Integer[]>>> siteOccupancyMappingBC = null;
    private Broadcast<Map<String, String>> siteTimeZoneMappingBC = null;
    private Broadcast<HashSet<String>> allLoadTypeSegmentsBC = null;
    private Broadcast<Map<String, Double>> siteSqftMappingBC = null;
    private Broadcast<Map<String, Integer>> siteInstallationYearsMappingBC = null;
    private Broadcast<Map<String, HashMap<Integer, String>>> sitePeakHoursMappingBC = null;
    private Broadcast<Map<String, String>> segmentSiteMappingBC = null;
    private Broadcast<Map<String, String>> loadTypeSegmentNameMappingBC = null;



	@Override
	public void broadcast(SparkContext sc, PropertiesBean propertiesBean) {
		System.out.println("**** Sending Broadcast assetSegmentMappingBC ****");
        try {
            apmDataLoader.generateMappings();
        } catch (Exception ex) {
            Map<String, String> configErrorMap = apmDataLoader.getConfigErrorMap();
            emailService.sendMessage("EM Out Of Sync Spark Job",
                        sc.applicationId(), configErrorMap, ex,
                        "EM Out of Sync Spark Job broadcast Alert.");
        }
        JavaSparkContext jsc = new JavaSparkContext(sc);
		assetEnterpriseMappingBC = jsc.broadcast(apmDataLoader.generateAssetEnterpriseMapping());
		assetSiteMappingBC = jsc.broadcast(apmDataLoader.generateAssetSiteMapping());
		siteEnterpriseMappingBC = jsc.broadcast(apmDataLoader.generateSiteEnterpriseMapping());
		segmentEnterpriseMappingBC = jsc.broadcast(apmDataLoader.generateSegmentEnterpriseMapping());
		assetSegmentMappingBC = jsc.broadcast(apmDataLoader.generateAssetSegmentMapping());

		assetSubmeterMappingBC = jsc.broadcast(apmDataLoader.generateAssetSegmentMapping());
		segmentSubmeterMappingBC = jsc.broadcast(apmDataLoader.getSegmentToSubmeterMapping());
		siteSubmeterMappingBC = jsc.broadcast(apmDataLoader.getSiteToSubmeterMapping());
		enterpriseSubmeterMappingBC = jsc.broadcast(apmDataLoader.getEnterpriseToSubmeterMapping());
		segmentLoadTypeMappingBC = jsc.broadcast(apmDataLoader.getLoadTypeMapping());
        billingListBC = jsc.broadcast(apmDataLoader.getBillingList());
        
        // for KPI Job broadcast
        siteOccupancyMappingBC = jsc.broadcast(apmDataLoader.generateSiteOccupancyMapping());
        siteTimeZoneMappingBC = jsc.broadcast(apmDataLoader.generateSiteTimeZoneMapping());
        allLoadTypeSegmentsBC = jsc.broadcast(new HashSet<>(apmDataLoader.getLoadTypeMapping().keySet()));
        siteSqftMappingBC = jsc.broadcast(apmDataLoader.generateSiteSqftMapping());
        siteInstallationYearsMappingBC = jsc.broadcast(apmDataLoader.generateSiteInstallationYearMapping());
        sitePeakHoursMappingBC = jsc.broadcast(apmDataLoader.generateSitePeakHoursMapping());
        segmentSiteMappingBC = jsc.broadcast(apmDataLoader.generateSegmentSiteMapping());
        loadTypeSegmentNameMappingBC = jsc.broadcast(apmDataLoader.getLoadTypeSegmentNameMapping   ());

	}

	@Override
	public void run(Environment env, String startTime, String endTime, String timeZone) {
        System.out.println("***** Out Of Sync Process Started for *****" + startTime);

        AggregationService aggregationService = new AggregationService();

		SparkContext sc = aggregationService.getCassandraConfiguredSparkContext(getAppName(), 
                              getPropertiesBean());
		CassandraSQLContext cassandraSQLContext = new CassandraSQLContext(sc);
        
		broadcast(sc, getPropertiesBean());
		processOutOfSync(sc, env, aggregationService, cassandraSQLContext, startTime);
	}

	private void processOutOfSync(SparkContext sc, Environment env, 
            AggregationService aggregationService, CassandraSQLContext cassandraSQLContext, String timeBucket) {

		String keySpace = propertiesBean.getValue(SparkConfigurations.CASSANDRA_KEYSPACE);
		String tableName = keySpace + ".out_of_sync_log";
		timeBucket = timeBucket.substring(0, 10);
		System.out.println("TIME BUCKET : " + timeBucket);
		String query = "Select yyyymmddhhmm, enterprise_uid, resrc_uid, event_type, time_bucket, resrc_tz from %s where time_bucket = '%s'";
		query = String.format(query, tableName, timeBucket);

		DataFrame dataFrame = cassandraSQLContext.cassandraSql(query).cache();

		if (dataFrame.count() > 0) {

			System.out.println("++++++++++++++++++++++++++++++++++++++++");
			System.out.println("..... Start Processing 15 MIN Job ......" + new Date() );
			System.out.println("++++++++++++++++++++++++++++++++++++++++");

			JavaRDD<Row> rows = dataFrame.toJavaRDD().cache();
			
			System.out.println("Count of Rows " + rows.count());

			JavaRDD<OutOfSyncLog> outOfSyncs = rows.map(new Function<Row, OutOfSyncLog>() {

				@Override
				public OutOfSyncLog call(Row row) throws Exception {
					String yyyyMMddHHmm = row.getString(0);

					OutOfSyncLog log = new OutOfSyncLog();
					log.setEnterprise_uid(row.getString(1));
					log.setEvent_type(row.getString(3));
					log.setResrc_uid(row.getString(2));
					log.setYyyymmddhhmm(row.getString(0));
					log.setTime_bucket(row.getString(4));
					log.setResrc_tz(row.getString(5));
					if (log.getResrc_type() == null) {
						log.setResrc_type("ASSET");
					}

                    // decide which job to run.
					switch (yyyyMMddHHmm.length()) {
						case 12:
							log.runForMin = true;
						case 10:
							log.runForHour = true;
						case 8:
							log.runForDay = true;
						case 6:
							log.runForMonth = true;
					}
					return log;
				}
			}).filter(oos -> oos != null);

			outOfSyncs.cache();
			
            processes15Min(cassandraSQLContext, keySpace, outOfSyncs);

			System.out.println("++++++++++++++++++++++++++++++++++++++++");
			System.out.println("..... Start Processing Hourly Job ......" + new Date());
			System.out.println("++++++++++++++++++++++++++++++++++++++++");
			
			
			processesHourly(sc, aggregationService, cassandraSQLContext, 
                    env, keySpace, outOfSyncs);

            
			System.out.println("++++++++++++++++++++++++++++++++++++++++");
			System.out.println("..... Start Processing Daily Job ......" + new Date());
			System.out.println("++++++++++++++++++++++++++++++++++++++++");

			Map<String, String> monthlyTimezoneMap = processesDaily(aggregationService, cassandraSQLContext, env, outOfSyncs);

			System.out.println("++++++++++++++++++++++++++++++++++++++++");
			System.out.println("..... Start Processing Monthly Job ......" + new Date());
			System.out.println("++++++++++++++++++++++++++++++++++++++++");

			processesMonthly(aggregationService, cassandraSQLContext, env, monthlyTimezoneMap);

			System.out.println("++++++++++++++++++++++++++++++++++++++++");
			System.out.println("..... CLOSING Rabbit MQ Connection ......" + new Date());
			System.out.println("++++++++++++++++++++++++++++++++++++++++");

            //close the channel
            try {
                IEAmqpUtil.closeConnection(false);
            } catch (Exception e) {
                LOG.error("Failed to close the AMQP connection.", e);
                LOG.error("Closing the connection forcefully");
                try {
                    IEAmqpUtil.closeConnection(true);
                } catch (Exception e1) {
                    LOG.error("CLosing the connection forcefully", e1);
                }
            }

			System.out.println("++++++++++++++++++++++++++++++++++++++++");
			System.out.println("..... COMPLETED ......" + new Date());
			System.out.println("++++++++++++++++++++++++++++++++++++++++");

		}
	}

    
	private void processesMonthly(AggregationService aggregationService, 
            CassandraSQLContext cassandraSQLContext, Environment env,
			Map<String, String> monthlyTimezoneMap) {
		// Fix Monthly Records
        /*
		List<Tuple2<String, OutOfSyncLog>> list =  outOfSyns
		.filter(o -> {
			return o.isRunForMonth();
		})
		.mapToPair(new PairFunction<OutOfSyncLog, String, OutOfSyncLog>() {
			@Override
			public Tuple2<String, OutOfSyncLog> call(OutOfSyncLog log) throws Exception {
				String tb = log.yyyymmddhhmm;
				// Just Get Day from timeBucket;
				tb.substring(0, 6);
				return new Tuple2<String, OutOfSyncLog>(tb.substring(0, 6) + log.resrc_tz, log);
			}
		})
		.reduceByKey(new Function2<OutOfSyncLog, OutOfSyncLog, OutOfSyncLog>() {

			@Override
			public OutOfSyncLog call(OutOfSyncLog v1, OutOfSyncLog v2) throws Exception {
				return v1;
			}
		})
		.collect();
        */
		
	    SparkCassandraMonthlyBatch monthlyBatch = new SparkCassandraMonthlyBatch();
        monthlyBatch.setMetaData(segmentLoadTypeMappingBC);

        for(String localMonth: monthlyTimezoneMap.keySet()) {
            String timezone = monthlyTimezoneMap.get(localMonth);
			monthlyBatch.processMonthly(aggregationService, cassandraSQLContext, env,
                    localMonth, localMonth, timezone);
        }



		//.parallelStream()
        /*
		for(Tuple2<String, OutOfSyncLog> oos : list){
			(oos -> {
			OutOfSyncLog os = oos._2;
			Date localTime = null;
			try {
				localTime = DateUtils.convertToUTC(os.yyyymmddhhmm.substring(0, 6), "yyyyMM");
			} catch (Exception e) {
				e.printStackTrace();
			}

            // Start the Monthly Batch 
			String localTimeString = DateUtils.convertUTCDateToLocalString(localTime, os.resrc_tz, "yyyyMM");

			monthlyBatch.processMonthly(aggregationService, cassandraSQLContext, env,
                    localTimeString, "", os.resrc_tz);
		});
        */
	}

	private Map<String, String> processesDaily(AggregationService aggregationService, 
            CassandraSQLContext cassandraSQLContext, Environment env, 
			JavaRDD<OutOfSyncLog> outOfSyns) {
		// Fix Daily Records

		List<Tuple2<String, OutOfSyncLog>> list =  outOfSyns.filter(o -> {
			return o.isRunForDay();
		}).mapToPair(new PairFunction<OutOfSyncLog, String, OutOfSyncLog>() {
			@Override
			public Tuple2<String, OutOfSyncLog> call(OutOfSyncLog log) throws Exception {
				String tb = log.yyyymmddhhmm;
				// Just Get Day from timeBucket;
				String utcHour = tb.substring(0, 10);
				return new Tuple2<String, OutOfSyncLog>(utcHour + log.resrc_tz, log);
			}
		}).reduceByKey(new Function2<OutOfSyncLog, OutOfSyncLog, OutOfSyncLog>() {
			@Override
			public OutOfSyncLog call(OutOfSyncLog v1, OutOfSyncLog v2) throws Exception {
				return v1;
			}
		}).collect();

		
	    SparkCassandraDailyBatch dailyBatch = new SparkCassandraDailyBatch();
        dailyBatch.setMetaData(segmentLoadTypeMappingBC);

        Map<String, String> monthlyTimezoneMap = new HashMap<>();


		for(Tuple2<String, OutOfSyncLog> tuple : list){
			OutOfSyncLog oos = tuple._2;
			
            String localDate = null;
            try {
                localDate = DateUtils.convertUTCStringToLocalTimeString(
                        oos.yyyymmddhhmm.substring(0, 10), "yyyyMMddHH",
                        oos.resrc_tz, "yyyyMMdd");
                System.out.println("*** Daily Batch: local Date = " + localDate
                        + "  origDate = " +  oos.yyyymmddhhmm.substring(0, 10));

                // build the monthly map with local month and local timezones. 
                String localMonth = localDate.substring(0, 6);
                if (! monthlyTimezoneMap.containsKey(localMonth)) {
                    monthlyTimezoneMap.put(localMonth, oos.resrc_tz);
                } else {
                    String currentTz = monthlyTimezoneMap.get(localMonth); 
                    System.out.println("*** currentTz = " + currentTz);
                    if (! currentTz.contains(oos.resrc_tz)) {
                        monthlyTimezoneMap.put(localMonth, currentTz + "," + oos.resrc_tz); 
                    }
                }

            } catch (Exception e) {
                System.out.println("*** Daily Batch: date can't be parsed, yyyyMMddHH = " 
                        + oos.yyyymmddhhmm.substring(0, 10));
                continue;
            }

            if (localDate == null) continue;

		    dailyBatch.processDailyEvents(aggregationService,cassandraSQLContext,
                    env, localDate, localDate, oos.resrc_tz);

            for(String key: monthlyTimezoneMap.keySet()) {
                System.out.println("*** Months to be run: month = " + key
                        + " *** timezone = " + monthlyTimezoneMap.get(key));
            }

		}
        return monthlyTimezoneMap;
	}

	private void processesHourly(SparkContext sc, AggregationService aggregationService,
            CassandraSQLContext cassandraSQLContext, 
			Environment env, String keySpace, JavaRDD<OutOfSyncLog> outOfSyns) {

        List<Tuple2<String, OutOfSyncLog>> list = outOfSyns.filter(o -> {
               return o.isRunForHour();
        }).mapToPair(log -> {
               return new Tuple2<String, OutOfSyncLog>(log.yyyymmddhhmm.substring(0, 10) + log.resrc_tz, log   );
        }).reduceByKey(new Function2<OutOfSyncLog, OutOfSyncLog, OutOfSyncLog>() {

               @Override
               public OutOfSyncLog call(OutOfSyncLog v1, OutOfSyncLog v2) throws Exception {
                   return v1;
               }
        }).collect();

		
		SparkCassandraHourlyBatch hourlyBatch = new SparkCassandraHourlyBatch();
        hourlyBatch.setMetaData(segmentLoadTypeMappingBC, 
               siteOccupancyMappingBC, siteTimeZoneMappingBC, siteSqftMappingBC,
               siteInstallationYearsMappingBC, sitePeakHoursMappingBC,
               segmentSiteMappingBC, segmentEnterpriseMappingBC,
               siteEnterpriseMappingBC, loadTypeSegmentNameMappingBC, 
               allLoadTypeSegmentsBC,
               propertiesBean);

        for(Tuple2<String, OutOfSyncLog> tuple : list){
            OutOfSyncLog oos = tuple._2; 

            //String yyyymmddhh = getLocalTimeString(oos.yyyymmddhhmm.substring(0, 10), oos.resrc_tz); 
            String yyyymmddhh = oos.yyyymmddhhmm.substring(0, 10);
            System.out.println("**** Run Hourly: hour = " + yyyymmddhh + " timezone = " + oos.resrc_tz);

			hourlyBatch.processHourlyEvents(sc, aggregationService, cassandraSQLContext, 
                    kpiCalculation, env, 
                    yyyymmddhh, yyyymmddhh, oos.resrc_tz);
		}
	}

    private String getLocalTimeString(String utcTimeString, String timezone) {
        Date localTime = null;
        try {
            localTime = DateUtils.convertToUTC(utcTimeString, "yyyyMMddHH");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        String localTimeString = DateUtils.convertUTCDateToLocalString(localTime, timezone, 
                "yyyyMMddHH");

        System.out.println("**** UTC hour = " + utcTimeString  + " local hour = " + localTime);
        return localTimeString;
    }


	private void processes15Min(CassandraSQLContext cassandraSQLContext,
            String keySpace, JavaRDD<OutOfSyncLog> outOfSyns) {

        // get the unique yyyymmddhhmm + timezone combination to run 15Mins aggregation
		List<Tuple2<String, OutOfSyncLog>> list = outOfSyns.filter(o -> {
			return o.isRunForMin();
		}).mapToPair(log -> {
			return new Tuple2<String, OutOfSyncLog>(log.yyyymmddhhmm + log.resrc_tz, log);
		}).reduceByKey((v1, v2) -> v1)
		.collect();

        if (list == null || list.size() == 0) {
            System.out.println("*** isRunForMin = false, No Need to run the 15mins job ***");
            return;
        }
		
	    MinuteBatch minBatch = new MinuteBatch();
        minBatch.setMetaData(assetSiteMappingBC, assetEnterpriseMappingBC, 
                siteEnterpriseMappingBC, segmentEnterpriseMappingBC, assetSegmentMappingBC,
                assetSubmeterMappingBC,segmentSubmeterMappingBC,siteSubmeterMappingBC,
                enterpriseSubmeterMappingBC,segmentLoadTypeMappingBC, 
                billingListBC, propertiesBean);

        // Hourly timezone map should contain the UTC hour time. 
        Map<String, String> aggregateHourlyTimezoneMap = new HashMap<String, String>();

		for(Tuple2<String, OutOfSyncLog> tuple : list){
			OutOfSyncLog oos = tuple._2;
            String hour = oos.yyyymmddhhmm.substring(0,10);
            String timezone = oos.resrc_tz;
            System.out.println("*** timezone = " + timezone);
            if (! aggregateHourlyTimezoneMap.containsKey(hour)) {
                aggregateHourlyTimezoneMap.put(hour, oos.resrc_tz);
            } else {
                String currentTz = aggregateHourlyTimezoneMap.get(hour); 
                System.out.println("*** currentTz = " + currentTz);
                if (! currentTz.contains(oos.resrc_tz)) {
                    aggregateHourlyTimezoneMap.put(hour, currentTz + "," + oos.resrc_tz); 
                }
            }

			try {
				JavaRDD<EventsByMinute> minuteEvents =  minBatch.process15MinsForResource(
                        cassandraSQLContext, keySpace, oos.yyyymmddhhmm);

                // send the 15mins record to billing queue.
                minBatch.sendMinEventsToQueue(minuteEvents, false);

			} catch (Exception e) {
				System.out.println("Error while processing 15 Mins Batch");
				e.printStackTrace();
			}
		}

        for(String key: aggregateHourlyTimezoneMap.keySet()) {
            System.out.println("*** key = " + key + 
                    " *** value = " + aggregateHourlyTimezoneMap.get(key));
        }
        //return aggregateHourlyTimezoneMap;
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
		//logger.error("startTimeString = " + timePair[0] + " endTimeString=" + endTimeString);
		return timePair;
	}

	@Override
	public PropertiesBean getPropertiesBean() {

		return propertiesBean;
	}

	@Override
	public String getMaster() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAppName() {
		// TODO Auto-generated method stub
		return "Out OF Sync Batch";
	}

}
