package com.ge.current.em.edgealarms.parser;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;

import com.daintreenetworks.haystack.commons.model.alarm.AlarmAction;
import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.em.analytics.dto.AlertReading;
import com.ge.current.em.analytics.dto.SiteOfflineReadingDTO;
import com.ge.current.em.entities.analytics.AlertLog;
import com.ge.current.ie.analytics.DbProperties;
import com.ge.current.ie.analytics.DbUtils;

public abstract class AbstractAlarmMessageParser {
	private static final Logger LOG = Logger.getLogger(AbstractAlarmMessageParser.class);
	

	public List<AlertLog> processAlarmData(byte[] payload, Broadcast<Map<String, String>> gatewaySiteMapping,
			Broadcast<Map<String, String>> siteTimezoneMapping, Broadcast<Map<String, String>> resourceNameMapping,
			Broadcast<Map<String, String>> siteEnterpriseMapping, DbProperties dbProperties) throws Exception {
		System.out.println("\nCalling getAlertsFromSiteReading");

		final DbUtils dbUtils = new DbUtils(dbProperties);
		List<SiteOfflineReadingDTO> dtos = filterAndSort(getSiteReadingDTO(payload, gatewaySiteMapping,
				siteTimezoneMapping));
		
		List<AlertLog> logs = new ArrayList<>();
		for (SiteOfflineReadingDTO dto : dtos) {
			final AlertLog newAlert = getAlertsFromSiteReading(dto, gatewaySiteMapping.value(), resourceNameMapping.value(), siteEnterpriseMapping.value());
			
			// We need to find the list of old alerts
			final AlertLog reading = dbUtils.findLastAlertLog(newAlert.getTime_bucket(), newAlert.getResrc_uid(), newAlert.getAlert_ts_tz());
			
			if (reading != null) {
				
				// Calculate this duration based on previous time stamp.
				// Assume duration is minute
				final Long diff = newAlert.getAlert_ts().getTime() - reading.getAlert_ts().getTime();
				if (diff <= 0) {
					// If diff is minute, we shouldn't add this to the list at all.
					System.err.println("Ignore adding alert, diff: " + diff + " as it's the old one:" + alertInfo(newAlert));
					continue;
				}
				
				// Set the diff
				final long seconds = ( diff / 1000) %60;
				final int duration = Long.valueOf(((diff-seconds)/1000)/60).intValue();
				newAlert.setDuration(duration);
				
				// Update the lastAlert to "PAST"
				reading.setStatus(AlarmConstants.ALERT_STATUS_PAST);
				
				// Add duration from previous alert
				newAlert.setDuration(reading.getDuration() + duration);
				
				System.out.println("Adding lastAlert...");
				printLog(reading);
				System.out.print("Adding new active...");
				printLog(newAlert);
				
				logs.add(reading);
				logs.add(newAlert);
			} else if (reading == null && newAlert.getStatus().equals(AlarmConstants.ALERT_STATUS_INACTIVE)) {
				System.out.println("Ignore inactive alert as there is nothing being active before:");
				printLog(newAlert);
			} else {
				System.out.print("No last event found, adding new active...");
				printLog(newAlert);
				logs.add(newAlert);
			}
			
			
		}
		
		if (!logs.isEmpty()) {
			DbUtils.saveLogs(logs);
		}
		
		return logs;
	}
	
	private void printLog(final AlertLog reading) {
		System.out.println(reading.getTime_bucket() + ", " + reading.getAlert_ts_loc() + ", " + reading.getResrc_uid() + ", " + reading.getSite_name() + ", " + reading.getStatus() + ", " + reading.getDuration());
	}
	
private List<SiteOfflineReadingDTO> filterAndSort(final List<SiteOfflineReadingDTO> logs) {
		
		// Should only have the last value of Alarm
		final Map<String, SiteOfflineReadingDTO> lastValueMap = new HashMap<>();
		
		
		for (SiteOfflineReadingDTO dto : logs) {
			if (dto != null) {
				// Depending on the site Id.
				lastValueMap.put(dto.getSiteUid(), dto);
			}
			
		}
		
		final List<SiteOfflineReadingDTO> list = new ArrayList<>(lastValueMap.values());
		Collections.sort(list);
		return list;
	}
	
	private String alertInfo(final AlertLog reading) {
		return  reading.getTime_bucket() + ", " + reading.getAlert_ts_loc() + ", " + reading.getResrc_uid() + ", " + reading.getSite_name() + ", " + reading.getStatus() + ", " + reading.getDuration();
	}
	
	
	private AlertLog getAlertsFromSiteReading(SiteOfflineReadingDTO siteReadingDTO,
			Map<String, String> gatewaySiteMapping, Map<String, String> resourceNameMapping,
			Map<String, String> siteEnterpriseMapping) {

		if (siteReadingDTO == null) {
			System.out.println("**** Invalid SiteReadingDTO ****");
			return null;
		}
		ZonedDateTime readingZonedDateTime = DateUtils.parseTime(siteReadingDTO.getTimeStamp(),
				AlarmConstants.ALARM_TIME_FORMAT);

		System.out.println("Report Time: " + readingZonedDateTime);

		String siteUid = gatewaySiteMapping.get(siteReadingDTO.getEdgeDeviceId());

		System.out.println("SiteUid:" + siteUid + " for " + siteReadingDTO.getEdgeDeviceId());

		// Merge all the measures for a given asset
		List<AlertLog> alertReadings = new ArrayList<>();
		AlertLog alertLog = new AlertLog();
		alertLog.setTime_bucket(DateUtils.getTimeBucket(readingZonedDateTime.toInstant().toEpochMilli()));
		alertLog.setSite_uid(siteUid);
		alertLog.setDuration(0);
		alertLog.setAlert_name(AlarmConstants.ALERT_NAME);
		alertLog.setAlert_type(AlarmConstants.ALERT_TYPE);
		alertLog.setCategory(AlarmConstants.ALERT_CATEGORY);
		alertLog.setSeverity(String.valueOf(AlarmConstants.SEVERITY));

		alertLog.setAlert_ts_tz(readingZonedDateTime.getZone().getId());
		alertLog.setAlert_ts(siteReadingDTO.getUtcTimeStamp());
		alertLog.setAlert_ts_loc(siteReadingDTO.getLocalTimeStamp());

		final String site = resourceNameMapping.get(siteUid);
		if (site != null) {
			alertLog.setSite_name(site);
		} else {
			alertLog.setSite_name("NONAME");
		}

		alertLog.setZone_name("NONAME");

		alertLog.setStatus(AlarmAction.RAISED.equals(siteReadingDTO.getAction()) ? AlarmConstants.ALERT_STATUS_ACTIVE : AlarmConstants.ALERT_STATUS_INACTIVE);
		alertLog.setLog_uuid(UUIDs.timeBased());

		alertLog.setEnterprise_uid(siteEnterpriseMapping.get(siteUid));
		alertLog.setResrc_uid(siteUid);
		alertLog.setResrc_type(AlarmConstants.RESOURCE_TYPE);

		alertReadings.add(alertLog);
		System.out.println("Read:" + alertLog.getTime_bucket() + ", " + alertLog.getAlert_ts_loc() + ", " + alertLog.getResrc_uid() + ", " + alertLog.getSite_name() + ", " + alertLog.getStatus() + ", " + alertLog.getDuration());
		
		return alertLog;
	}

	private Stream<AlertReading> getAlertsFromSiteReading(SiteOfflineReadingDTO siteReadingDTO, 
			Broadcast<Map<String, String>> gatewaySiteMapping) {

		if (siteReadingDTO == null) {
			System.out.println("**** Invalid SiteReadingDTO ****");
			return null;
		}
		ZonedDateTime readingZonedDateTime = DateUtils.parseTime(siteReadingDTO.getTimeStamp(),
				AlarmConstants.ALARM_TIME_FORMAT);

		System.out.println("Report Time: " + readingZonedDateTime);

		String siteUid = gatewaySiteMapping.value().get(siteReadingDTO.getEdgeDeviceId());

		System.out.println("SiteUid:" + siteUid + " for " + siteReadingDTO.getEdgeDeviceId());

		// Merge all the measures for a given asset
		List<AlertReading> alertReadings = new ArrayList<>();
		alertReadings.add(new AlertReading().setSiteUid(siteUid).setAction(siteReadingDTO.getAction())
				.setEventTs(readingZonedDateTime.toInstant().toEpochMilli())
				.setEventTsUtc(siteReadingDTO.getUtcTimeStamp())
				.setEventTsLocal(siteReadingDTO.getLocalTimeStamp())
				.setZonedDateTime(readingZonedDateTime));
		
		System.out.println("Created AlertReading:" + ReflectionToStringBuilder.toString(alertReadings.get(0)));
		return alertReadings.stream();
	}

	protected abstract List<SiteOfflineReadingDTO> getSiteReadingDTO(byte[] payload,
			Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, String>> siteTimezoneMapping)
			throws IOException;
}
