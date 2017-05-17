/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.em.edgealarms.parser;

import static com.ge.current.em.analytics.dto.PointDTO.PointDTOBuilder.aPointDTO;
import static com.ge.current.em.analytics.dto.SiteReadingDTO.SiteReadingDTOBuilder.aSiteReadingDTO;

import java.io.IOException;
import java.io.Serializable;

import java.time.format.DateTimeFormatter;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.ge.current.em.analytics.dto.PointDTO;
import com.ge.current.em.analytics.dto.SiteOfflineReadingDTO;
import com.ge.current.em.analytics.dto.SiteReadingDTO;
import com.ge.current.em.analytics.dto.daintree.attribute.Attribute;
import com.ge.current.em.analytics.dto.daintree.attribute.PointValue;
import com.ge.current.em.analytics.dto.daintree.attribute.Unit;
import com.ge.current.em.analytics.dto.daintree.attribute.Value;
import com.ge.current.em.analytics.dto.daintree.dto.AlarmEntry;
import com.ge.current.em.analytics.dto.daintree.dto.AlarmMessageDTO;
import com.ge.current.em.analytics.dto.daintree.dto.EventEntry;
import com.ge.current.em.analytics.dto.daintree.dto.TimeSeriesLogDTO;
import com.ge.current.em.analytics.dto.daintree.dto.TimeSeriesMessageDTO;

public class WacAlarmMessageParser extends AbstractAlarmMessageParser implements Serializable {
	private static final Logger system = Logger.getLogger(WacAlarmMessageParser.class);
	private static final long serialVersionUID = -6916385118646003582L;

	private static final String TIME_SERIES_LOG_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
	private static final String CSM_ALARM_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final org.joda.time.format.DateTimeFormatter DTF = org.joda.time.format.DateTimeFormat
			.forPattern(CSM_ALARM_DATE_TIME_FORMAT).withZoneUTC();
	private static final org.joda.time.format.DateTimeFormatter ALARM_DTF = org.joda.time.format.DateTimeFormat
			.forPattern(TIME_SERIES_LOG_DATE_TIME_FORMAT).withZoneUTC();
	private static final DateTimeFormatter utcFormatter = DateTimeFormatter.ofPattern(TIME_SERIES_LOG_DATE_TIME_FORMAT)
			.withZone(ZoneId.of("UTC"));

	@Override
    protected List<SiteOfflineReadingDTO> getSiteReadingDTO(byte[] payload, 
            Broadcast<Map<String, String>> gatewaySiteMapping, 
            Broadcast<Map<String, String>> siteTimezoneMapping) throws IOException {
        List<AlarmEntry> logs = mapPayloadToLogs(payload);
        
        final List<SiteOfflineReadingDTO> siteAlarms = logs.stream().map(log -> toSiteReadingDTO(log, 
                gatewaySiteMapping, siteTimezoneMapping)).collect(Collectors.toList());
        
        return siteAlarms;
    }

	
	protected List<AlarmEntry> mapPayloadToLogs(byte[] payload) throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

		AlarmMessageDTO messageDTO = objectMapper.readValue(payload, AlarmMessageDTO.class);
		List<AlarmEntry> logs = messageDTO.getData();
		if (logs == null) {
			return Collections.emptyList();
		} else {
			return processEntries(logs);
		}
	}

	private List<AlarmEntry> processEntries(final List<AlarmEntry> logs) {
		Collections.sort(logs);

		// Sort all the alarm by entry
		final Map<String, AlarmEntry> siteAlarmMap = new HashMap<>();
		for (AlarmEntry log : logs) {

			if (AlarmConstants.OFFLINE_ALARMS.contains(log.getAlarmId())) {

				// There should be only one alarm per site at any time.
				siteAlarmMap.put(log.getFacilityID(), log);
			}
		}

		return new ArrayList<>(siteAlarmMap.values());
	}

	public SiteOfflineReadingDTO toSiteReadingDTO(AlarmEntry dto, Broadcast<Map<String, String>> gatewaySiteMapping,
			Broadcast<Map<String, String>> siteTimezoneMapping) {
		AlertData reportTime = formatReportTime(dto.getFacilityID(), dto.getTimeRef(), gatewaySiteMapping,
				siteTimezoneMapping);
		// if the time can't be parsed or the edge device doesn't exist.
		if (reportTime == null)
			return null;

		return new SiteOfflineReadingDTO().setEdgeDeviceId(dto.getFacilityID()).setTimeStamp(reportTime.localTime)
				.setUtcTimeStamp(reportTime.utcTs).setLocalTimeStamp(reportTime.localTs).setSiteUid(reportTime.siteUid)
				.setAction(dto.getAction());
	}

	private AlertData formatReportTime(String edgeDeviceID, String timeRef,
			Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, String>> siteTimezoneMapping) {
		if (timeRef == null) {
			return null;
		}

		// WAC will use the Site timezone configured in APM.
		String siteTimezone = null;
		String siteUid = null;
		if (gatewaySiteMapping.value().containsKey(edgeDeviceID)) {
			siteUid = gatewaySiteMapping.value().get(edgeDeviceID);
			if (siteTimezoneMapping.value().containsKey(siteUid)) {
				siteTimezone = siteTimezoneMapping.value().get(siteUid);
			}
			if (siteTimezone == null) {
				System.out.println("No time Zone configured for gateway: " + edgeDeviceID);
				if (siteUid != null) {
					System.out.println("No time Zone configured for Site: " + siteUid);
				}
				return null;
			}
			System.out.println("Timezone for site: " + siteUid + " siteTimezone = " + siteTimezone);
		} else {
			System.out.println("EdgeDevice ID: " + edgeDeviceID + " does not exist in mapping.");
		}

		if (siteTimezone == null) {
			System.out.println("*** Timezone is null for site " + siteUid);
			return null;
		}

		DateTimeFormatter JACE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(AlarmConstants.ALARM_TIME_FORMAT)
				.withZone(ZoneId.of(siteTimezone));
		;
		final Date utcTime = DTF.parseDateTime(timeRef).toDate();

		LocalDateTime ldt = LocalDateTime.parse(ALARM_DTF.print(utcTime.getTime()), utcFormatter);

		ZonedDateTime zdt = ldt.atZone(ZoneId.of(siteTimezone));
		ZoneOffset offset = zdt.getOffset();
		ldt = ldt.plus(offset.getTotalSeconds(), ChronoUnit.SECONDS);
		zdt = ZonedDateTime.of(ldt, ZoneId.of(siteTimezone));
		System.out.println("**** zdt =  " + zdt.format(JACE_DATE_TIME_FORMATTER));
		AlertData alertTime = new AlertData();
		alertTime.localTime = zdt.format(JACE_DATE_TIME_FORMATTER);
		alertTime.localTs = Date.from(ldt.atZone(ZoneId.of(siteTimezone)).toInstant());
		alertTime.utcTs = utcTime;
		alertTime.siteUid = siteUid;
		return alertTime;
	}

	private static class AlertData {
		private String localTime;
		private Date localTs;
		private Date utcTs;
		private String siteUid;

	}
}
