/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.em.edgealarms.parser;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.broadcast.Broadcast;

import com.daintreenetworks.haystack.commons.model.alarm.AlarmAction;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.em.analytics.dto.SiteOfflineReadingDTO;

public class JaceAlarmMessageParser extends AbstractAlarmMessageParser implements Serializable {
	private static final long serialVersionUID = -6916385118646003582L;
    protected static final String JACE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX VV";
	
	@Override
	protected List<SiteOfflineReadingDTO> getSiteReadingDTO(byte[] payload,
			Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, String>> siteTimezoneMapping)
			throws IOException {
		List<EdgeAlarmDTO> logs = new ArrayList<>();
		logs.add(mapPayloadToLogs(payload));

		final List<SiteOfflineReadingDTO> siteAlarms = logs.stream()
				.map(log -> toSiteReadingDTO(log, gatewaySiteMapping, siteTimezoneMapping))
				.collect(Collectors.toList());

		return siteAlarms;
	}

	protected EdgeAlarmDTO mapPayloadToLogs(byte[] payload) throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

		try {
			return objectMapper.readValue(payload, EdgeAlarmDTO.class);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public SiteOfflineReadingDTO toSiteReadingDTO(EdgeAlarmDTO dto, Broadcast<Map<String, String>> gatewaySiteMapping,
			Broadcast<Map<String, String>> siteTimezoneMapping) {
		AlertData reportTime = formatReportTime(dto.getEdgeDeviceID(), dto.getTime(), gatewaySiteMapping, siteTimezoneMapping);
		// if the time can't be parsed or the edge device doesn't exist.
		if (reportTime == null)
			return null;

		return new SiteOfflineReadingDTO().setEdgeDeviceId(dto.getEdgeDeviceID()).setTimeStamp(reportTime.localTime)
				.setUtcTimeStamp(reportTime.utcTs).setLocalTimeStamp(reportTime.localTs).setSiteUid(reportTime.siteUid)
				.setAction(
						AlarmAction.RAISED.name().equals(dto.getAction()) ? AlarmAction.RAISED : AlarmAction.CLEARED);
	}

	private AlertData formatReportTime(String edgeDeviceID, String timeRef,
			Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, String>> siteTimezoneMapping) {
		
		final String siteUid = gatewaySiteMapping.value().get(edgeDeviceID);
		ZonedDateTime readingZonedDateTime = null;
		if (timeRef == null || timeRef.isEmpty()) {
			System.out.println("No timeref. using local current time");
			
			final String timezone = siteTimezoneMapping.value().get(siteUid);
			if (timezone == null) {
				System.err.println("Time Zone not found for " + siteUid);
				return null;
			}
			LocalDateTime ldt = LocalDateTime.now(ZoneId.of(timezone));
			readingZonedDateTime = ldt.atZone(ZoneId.of(timezone));
			
			DateTimeFormatter JACE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(JACE_TIME_FORMAT).withZone(ZoneId.of(timezone));
		    timeRef = readingZonedDateTime.format(JACE_DATE_TIME_FORMATTER);
		    System.out.println("Using current local time:" + timeRef);
		} else {
			if (timeRef.contains("[")) {
				timeRef = timeRef.replace("[", " ").replace("]", "");
			}
			readingZonedDateTime = DateUtils.parseTime(timeRef, JACE_TIME_FORMAT);
		}
		
		AlertData alertTime = new AlertData();
		alertTime.localTime = timeRef;
		alertTime.localTs = Date.from(readingZonedDateTime.toInstant());
		
		final ZonedDateTime utc = ZonedDateTime.of(readingZonedDateTime.toLocalDateTime(), ZoneOffset.UTC);
		alertTime.utcTs = Date.from(utc.toInstant());
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
