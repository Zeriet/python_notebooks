/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.em.aggregation.parser;

import static com.ge.current.em.aggregation.request.SiteReadingDTOContainer.SiteReadingDTOContainerBuilder.aSiteReadingDTOContainer;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import com.ge.current.em.aggregation.request.SiteReadingDTOContainer;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.ge.current.em.analytics.dto.PointDTO;
import com.ge.current.em.analytics.dto.PointReadingDTO;
import com.ge.current.em.analytics.dto.SiteReadingDTO;
import com.ge.current.em.analytics.dto.daintree.attribute.Attribute;
import com.ge.current.em.analytics.dto.daintree.attribute.PointValue;
import com.ge.current.em.analytics.dto.daintree.attribute.Unit;
import com.ge.current.em.analytics.dto.daintree.attribute.Value;
import com.ge.current.em.analytics.dto.daintree.dto.EventEntry;
import com.ge.current.em.analytics.dto.daintree.dto.TimeSeriesLogDTO;
import com.ge.current.em.analytics.dto.daintree.dto.TimeSeriesMessageDTO;

import com.ge.current.em.aggregation.NormalizationConstants;

public class WacMessageParser extends AbstractMessageParser implements Serializable {
	private static final Logger LOG = Logger.getLogger(WacMessageParser.class);
	private static final long serialVersionUID = -6916385118646003582L;

	private static final String TIME_SERIES_LOG_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
	private static final DateTimeFormatter utcFormatter = DateTimeFormatter.ofPattern(TIME_SERIES_LOG_DATE_TIME_FORMAT)
			.withZone(ZoneId.of("UTC"));

	@Override
	protected List<SiteReadingDTOContainer> getSiteReadingDTO(byte[] payload, 
            Broadcast<Map<String, String>> gatewaySiteMapping,
			Broadcast<Map<String, String>> siteTimezoneMapping) throws IOException {

        List<TimeSeriesLogDTO> logs = null;
        try {
		    logs = mapPayloadToLogs(payload);
        } catch (Exception e) {
            return Collections.singletonList(aSiteReadingDTOContainer()
                                                .withInvalidReading()
                                                .withErrorMessage(ExceptionUtils.getStackTrace(e))
                                                .withRawSiteReading(new String(payload))
                                                .build());
        }
        LOG.info("Total " + logs.size() +" messages received.");
        return logs.stream()
            .map(log -> aSiteReadingDTOContainer()
                        .withValidReading()
                        .withSiteReading(getSiteReadingDTOFromLog(log, 
                                gatewaySiteMapping, siteTimezoneMapping))
                        .build())
            .collect(Collectors.toList());
	}

    private SiteReadingDTO getSiteReadingDTOFromLog(TimeSeriesLogDTO log, 
                    Broadcast<Map<String, String>> gatewaySiteMapping, 
                    Broadcast<Map<String, String>> siteTimezoneMapping) {
        return toSiteReadingDTO(log, gatewaySiteMapping, siteTimezoneMapping);
    }



	protected List<TimeSeriesLogDTO> mapPayloadToLogs(byte[] payload) throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

		TimeSeriesMessageDTO messageDTO = objectMapper.readValue(payload, TimeSeriesMessageDTO.class);
		List<TimeSeriesLogDTO> logs = messageDTO.getData();
		if (logs == null) {
			return Collections.emptyList();
		} else {
			final List<TimeSeriesLogDTO> results = new ArrayList<>();
			for (TimeSeriesLogDTO dto : logs) {

				// Only add if there's any events and events has timeRef.
				if (dto.getData() != null && !dto.getData().isEmpty() && dto.getData().get(0).getTimeRef() != null
						&& !dto.getData().get(0).getTimeRef().isEmpty()) {
					results.add(dto);
				}
			}
			return results;
		}
	}

	private static String getKey(final TimeSeriesLogDTO log) {
		return log.getZoneID() + ":" + log.getTimeRef();
	}

	public SiteReadingDTO toSiteReadingDTO(TimeSeriesLogDTO dto, Broadcast<Map<String, String>> gatewaySiteMapping,
			Broadcast<Map<String, String>> siteTimezoneMapping) {

		// WAC will use the Site timezone configured in APM.
		String siteTimezone = null;
		String siteUid = null;

		if (gatewaySiteMapping.value().containsKey(dto.getFacilityID())) {
			siteUid = gatewaySiteMapping.value().get(dto.getFacilityID());
			if (siteTimezoneMapping.value().containsKey(siteUid)) {
				siteTimezone = siteTimezoneMapping.value().get(siteUid);
			}
			if (siteTimezone == null) {
				LOG.info("No time Zone configured for gateway: " + dto.getFacilityID());
				if (siteUid != null) {
					LOG.info("No time Zone configured for Site: " + siteUid);
				}
				return null;
			}
			LOG.info("Timezone for site: " + siteUid + " siteTimezone = " + siteTimezone);
		} else {
			LOG.info("EdgeDevice ID: " + dto.getFacilityID() + " does not exist in mapping. Ignore processing");
			return null;
		}

		DateTimeFormatter JACE_DATE_TIME_FORMATTER = DateTimeFormatter
				.ofPattern(NormalizationConstants.JACE_TIME_FORMAT).withZone(ZoneId.of(siteTimezone));
		
		
		String reportTime = formatReportTime(JACE_DATE_TIME_FORMATTER, siteTimezone, dto.getLocalTimeRef());
		
		final SiteReadingDTO reading = aSiteReadingDTO().withEdgeDeviceId(dto.getFacilityID()).withEdgeDeviceType("WAC")
				.withReportTime(reportTime).withPoints(formatPoints(dto, JACE_DATE_TIME_FORMATTER, siteTimezone, reportTime)).build();
		LOG.info("SiteReading:" + ReflectionToStringBuilder.toString(reading, ToStringStyle.SIMPLE_STYLE));
		return reading;
	}
	
	private String formatReportTime(DateTimeFormatter JACE_DATE_TIME_FORMATTER, final String siteTimezone, final String localTime) {
		LocalDateTime ldt = LocalDateTime.parse(localTime, utcFormatter);
		ZonedDateTime zdt = ldt.atZone(ZoneId.of(siteTimezone));
		ZoneOffset offset = zdt.getOffset();
		ldt = ldt.plus(offset.getTotalSeconds(), ChronoUnit.SECONDS);
		zdt = ZonedDateTime.of(ldt, ZoneId.of(siteTimezone));
		LOG.info("**** zdt =  " + zdt.format(JACE_DATE_TIME_FORMATTER) + ", org:" + localTime);

		return zdt.format(JACE_DATE_TIME_FORMATTER);
		
	}

	private List<PointDTO> formatPoints(TimeSeriesLogDTO dto, DateTimeFormatter JACE_DATE_TIME_FORMATTER, final String siteTimezone, final String reportTime) {
		// Sort all by desc first to show the last value in the event.
		Collections.sort(dto.getData());
		
		final Map<String, PointDTO> pointMap = new HashMap<>();
		for (EventEntry value : dto.getData()) {
			final String pointName = formatPointName(dto.getZoneID(), value.getAttribute());
			final Attribute attr = value.getAttrType();
			PointDTO pointDto = pointMap.get(pointName);
			final PointValue point = attr.apply(value);
			final String pointValue = String.valueOf(point.getValue() != null ? point.getValue() : "");
			if (pointDto == null) {
				// Create new point.
				pointDto = aPointDTO().withPointName(formatPointName(dto.getZoneID(), value.getAttribute()))
						.withCurStatus("OK")
						.withPointType(point.getType().name()).withPointReadings(new ArrayList<>()).build();
				pointMap.put(pointName, pointDto);
			}

			// Set the current value to the latest.
			pointDto.setCurValue(pointValue);
			
			// Add all the other points
			pointDto.getPointReadings().add(createPointReading(pointValue, formatReportTime(JACE_DATE_TIME_FORMATTER, siteTimezone, value.getTimeRefLocal())));
		}
		
		// Add the last value if the reading doesn't include the last value.
		// May be this step is not needed.
		final List<PointDTO> allPoints = new ArrayList<PointDTO>(pointMap.values());
 		for (PointDTO point : allPoints) {
 			boolean includeLastValue = false;
 			for (PointReadingDTO reading : point.getPointReadings()) {
 				if (reading.getTimeStamp().equals(reportTime)) {
 					includeLastValue = true;
 					break;
 				}
 			}
 			if (!includeLastValue) {
 				point.getPointReadings().add(createPointReading(point.getCurValue(), reportTime));
 			}
 		}
		return allPoints;
	}
	
	private PointReadingDTO createPointReading(final Object pointValue, String timeStamp) {
		final PointReadingDTO pointReading = new PointReadingDTO();
		pointReading.setStatus("OK");
		pointReading.setValue(pointValue);
		pointReading.setTimeStamp(timeStamp);
		LOG.info("**** reading:" + pointReading.getValue() + ", " + pointReading.getTimeStamp());
		return pointReading;
	}

	private String formatPointName(String zoneId, String attribute) {
		return "/Drivers/DaintreeNetwork/" + zoneId + "/points/" + attribute;
	}
}
