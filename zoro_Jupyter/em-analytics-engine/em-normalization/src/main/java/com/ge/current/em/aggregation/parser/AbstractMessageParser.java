/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.em.aggregation.parser;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.aggregation.request.EventContainer;
import com.ge.current.em.aggregation.request.SiteReadingDTOContainer;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.model.nosql.MessageLog;
import com.ge.current.ie.util.CassandraUtil;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;

import com.ge.current.em.aggregation.NormalizationConstants;
import com.ge.current.em.aggregation.utils.JaceUtil;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.em.analytics.dto.JaceEvent;
import com.ge.current.em.analytics.dto.PointObject;
import com.ge.current.em.analytics.dto.SiteReadingDTO;

import static com.ge.current.em.aggregation.request.EventContainer.EventContainerBuilder.anEventContainer;

abstract class AbstractMessageParser {
    private static final Logger LOG = Logger.getLogger(JACEMessageParser.class);
    private CassandraUtil cassandraUtil = new CassandraUtil();

    public List<EventContainer> parseMessage(byte[] payload, PropertiesBean propertiesBean, JaceUtil jaceUtil, Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, Map<String, PointObject>>> siteAssetnameMapping, Broadcast<Map<String, String>> siteTimezoneMapping) throws Exception {
        List<SiteReadingDTOContainer> siteReadingStream = getSiteReadingDTO(payload, gatewaySiteMapping, siteTimezoneMapping);
        List<EventContainer> invalidEvents = siteReadingStream.stream()
                                                              .filter(siteReadingDTOContainer -> !siteReadingDTOContainer.isValidSiteReading())
                                                              .map(invalidSiteReading -> anEventContainer()
                                                                             .withInvalidEventStatus()
                                                                             .withReasonPhrase(invalidSiteReading.getErrorMsg().get())
                                                                             .withRawEvent(invalidSiteReading.getRawSiteReading().get())
                                                                             .build())
                                                              .collect(Collectors.toList());
        LOG.info("Invalid events: " + invalidEvents.size());

        List<EventContainer> validEvents = siteReadingStream.stream()
                                                      .filter(siteReadingDTOContainer -> siteReadingDTOContainer.isValidSiteReading())
                                                      .flatMap(dto -> getEventsFromSiteReading(dto.getSiteReadingDTO(), propertiesBean, jaceUtil, gatewaySiteMapping, siteAssetnameMapping))
                                                            .collect(Collectors.toList());

        List<EventContainer> allEvents = validEvents;
        allEvents.addAll(invalidEvents);

        LOG.info("All events: " + allEvents.size());
        return allEvents;
    }

        // Merge all the measures for a given asset
    private Stream<EventContainer> getEventsFromSiteReading(SiteReadingDTO siteReadingDTO, PropertiesBean propertiesBean, JaceUtil jaceUtil, Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, Map<String, PointObject>>> siteAssetnameMapping) {
        if (siteReadingDTO == null) {
            System.out.println("**** Invalid SiteReadingDTO ****");
            return null;
        }

        ZonedDateTime readingZonedDateTime = DateUtils.parseTime(siteReadingDTO.getReportTime(), NormalizationConstants.JACE_TIME_FORMAT);

        List<EventContainer> jaceEvents = new ArrayList<>();
        if(gatewaySiteMapping == null || siteAssetnameMapping == null) {
            String errorMsg = "Gateway site mapping or site asset Reading ignored. Edge Device id: " + siteReadingDTO.getEdgeDeviceId();
            String errorSummary = String.format("%s", siteReadingDTO.getEdgeDeviceId());
            writeErrorToCassandra(propertiesBean, errorSummary, errorMsg, Date.from(readingZonedDateTime.toInstant()));
            LOG.error(errorMsg);

            return jaceEvents.stream();
        }

        LOG.info("Report Time: " + readingZonedDateTime);

        // Generate one JaceEvent per asset measures so there will be multiple
        // jaceevents for every measure in a asset.
        Map<String, List<JaceEvent>> assetsMeasures = new HashMap<>();
        for (com.ge.current.em.analytics.dto.PointDTO point : siteReadingDTO.getPoints()) {

            // @formatter: off
            PointObject pointMetaData = jaceUtil.getPointObjectFromPointName(siteReadingDTO.getEdgeDeviceId(),
                                                                             point.getPointName(),
                                                                             gatewaySiteMapping.value(),
                                                                             siteAssetnameMapping.value());
            // @formatter: on

            if (pointMetaData == null) {
                String errorMsg = "Failed to get point meta data. Reading ignored. Edge Device id: " + siteReadingDTO.getEdgeDeviceId() + ". Point Name: " + point.getPointName();
                String errorSummary = String.format("%s, %s", siteReadingDTO.getEdgeDeviceId(), point.getPointName());
                writeErrorToCassandra(propertiesBean, errorSummary, errorMsg, Date.from(readingZonedDateTime.toInstant()));
                LOG.error(errorMsg);

                continue;
            }

            LOG.info("Point: " + point + " meta data: " + pointMetaData);
            String currTime = siteReadingDTO.getReportTime();
            int readingInterval = siteReadingDTO.getReportingIntervalInSeconds();

            JaceEvent jaceEvent = null;
            try {
                jaceEvent = jaceUtil.getJaceEventFromPoint(point, pointMetaData, readingZonedDateTime, readingInterval, currTime);
            }catch(Exception e) {
                String errorSummary = String.format("%s, %s, %s, %s", point.getPointName(), pointMetaData.getAssetSourceKey(), pointMetaData.getHaystackName(), e.getMessage());
                writeErrorToCassandra(propertiesBean, errorSummary, ExceptionUtils.getStackTrace(e), Date.from(readingZonedDateTime.toInstant()));
                LOG.error(errorSummary);
                continue;
            }

            // This will happen if the message is valid but the jace event status
            // is not "ok".
            if (jaceEvent == null) {
                continue;
            }

            jaceEvent.setAssetUid(pointMetaData.getAssetSourceKey());
            jaceEvent.setZonedDateTime(readingZonedDateTime);
            LOG.info("*** set event Ts = " + readingZonedDateTime.toInstant().toEpochMilli());
            jaceEvent.setEventTs(readingZonedDateTime.toInstant().toEpochMilli());

            if (assetsMeasures.get(pointMetaData.getAssetSourceKey()) == null) {
                assetsMeasures.put(pointMetaData.getAssetSourceKey(), new ArrayList<>());
            }

            assetsMeasures.get(pointMetaData.getAssetSourceKey()).add(jaceEvent);
        }


        for (Map.Entry<String, List<JaceEvent>> assetMeasures : assetsMeasures.entrySet()) {
            JaceEvent assetJaceEvent = new JaceEvent();

            assetJaceEvent.setAssetUid(assetMeasures.getKey());

            for (JaceEvent jaceEvent : assetMeasures.getValue()) {
                assetJaceEvent.setZonedDateTime(jaceEvent.getZonedDateTime());
                assetJaceEvent.setEventTs(jaceEvent.getEventTs());
                assetJaceEvent.setEventType("reading");
                assetJaceEvent.getMeasures().putAll(jaceEvent.getMeasures());
                assetJaceEvent.getTags().putAll(jaceEvent.getTags());
                //*** set all the rawMeasures and rawTags ***
                assetJaceEvent.getRawMeasures().putAll(jaceEvent.getRawMeasures());
                assetJaceEvent.getRawTags().putAll(jaceEvent.getRawTags());
            }

            jaceEvents.add(anEventContainer()
                            .withValidEventStatus()
                            .withEvent(assetJaceEvent)
                            .build());
        }

        if (jaceEvents.size() > 0) {
            LOG.error("**** JACE EVENTS = " + jaceEvents);
        }
        return jaceEvents.stream();
    }

    protected void writeErrorToCassandra(PropertiesBean propertiesBean, String errorSummary, String detailedErrorMsg, Date ingestionTime) {
        LOG.error(detailedErrorMsg);

        MessageLog messageLog = new MessageLog();
        messageLog.setProcessUid("Normalization");
        messageLog.setErrorTs(ingestionTime);
        messageLog.setLogUUID(UUIDs.timeBased());
        messageLog.setAppKey("Normalization");
        messageLog.setMsg_sumry(errorSummary);
        messageLog.setMsg(detailedErrorMsg);

        cassandraUtil.writeToCassandra(propertiesBean, messageLog);
    }

    protected abstract List<SiteReadingDTOContainer> getSiteReadingDTO(byte[] payload, Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, String>> siteTimezoneMapping) throws IOException;
}
