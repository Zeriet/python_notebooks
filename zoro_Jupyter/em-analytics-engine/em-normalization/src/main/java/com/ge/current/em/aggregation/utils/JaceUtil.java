package com.ge.current.em.aggregation.utils;

import com.ge.current.em.aggregation.NormalizationConstants;
import com.ge.current.em.analytics.dto.JaceEvent;
import com.ge.current.em.analytics.dto.PointDTO;
import com.ge.current.em.analytics.dto.PointObject;
import com.ge.current.em.analytics.dto.PointReadingDTO;
import com.ge.current.em.analytics.common.DateUtils;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by 212582112 on 1/25/17.
 */
public class JaceUtil implements Serializable {
    private static Logger LOG = Logger.getLogger(JaceUtil.class);

    public PointObject getPointObjectFromPointName(String gatewayId, String pointName, 
            Map<String, String> gatewaySiteMapping, 
            Map<String, Map<String, PointObject>> siteAssetnameMapping) {

        String siteUid = gatewaySiteMapping.get(gatewayId);
        if(siteUid == null || !siteAssetnameMapping.containsKey(siteUid))  return null;
        return siteAssetnameMapping.get(siteUid).get(pointName);
    }

    public JaceEvent getJaceEventFromPoint(PointDTO point, PointObject pointMetaData, ZonedDateTime readingZonedDateTime, int readingInterval, String currTime) {
        if (point == null || pointMetaData == null) return null;

        String curStatus = point.getCurStatus();
        if (!curStatus.equalsIgnoreCase("ok")) {
            LOG.error("Point Value is not OK. Point: " + point);
            return null;
        }

        String pointName = pointMetaData.getHaystackName();
        System.out.println("=== pointName = " + pointName);

        Map<String, Double> measures = new HashMap<>();
        Map<String, Double> rawMeasures = new HashMap<>();
        Map<String, String> tags = new HashMap<>();
        Map<String, String> rawTags = new HashMap<>();

        String pointType = point.getPointType();
        Double currValue = null;
        if (pointType.equalsIgnoreCase("numeric")){
            try {
                currValue = Double.valueOf(String.valueOf(point.getCurValue()));
                measures.put(pointName, currValue);

                rawMeasures.put(point.getPointName(), currValue); 
                System.out.println("=== Raw Measures PointName = " + point.getPointName()
                        + " pointValue = " + currValue);
            } catch (Exception e) {
                LOG.error("Number Format error in : " +  pointName +" value: " + 
                        point.getCurValue());
            }

            if (currValue != null && pointMetaData.getHaystackName().toLowerCase().
                     contains("zoneelecmeterenergysensor")) {
                Double kWhValue = calculateKwhValue(currValue, point);
                if (kWhValue != null) {
                    measures.put("kWh", kWhValue);
                }
            }
        } else {
            String pointValue = (String) (point.getCurValue());
            System.out.println("=== pointValue = " + pointValue);

            tags.put(pointName, pointValue.toLowerCase());
            rawTags.put(point.getPointName(), pointValue.toLowerCase());
            System.out.println("=== Raw Tag PointName = " + point.getPointName()
                        + " pointValue = " + pointValue);

            Map<String, String> lastRuntime = new HashMap();

            Map<String, Double> runtimeMeasures = calculateTagRuntime(pointName, pointValue, 
                          point, readingInterval, currTime, lastRuntime);

            // in measures, we keep the runtime and total count for each state of the tags. 
            // so we can do the measures aggregation on all the runtimes. 
            measures.putAll(runtimeMeasures);

            // in tags, we only keep the latest run time for each tag. 
            tags.putAll(lastRuntime);
        }

        // @formatter: off
        JaceEvent jaceEvent = new JaceEvent(pointMetaData.getAssetSourceKey(),
                             "reading",
                             measures,
                             tags,
                             readingZonedDateTime.toInstant().toEpochMilli(),
                             readingZonedDateTime);

        System.out.println("*** rawMeasurs size = " + rawMeasures.size()
                + " *** rawTags size = " + rawTags.size());
        jaceEvent.setRawMeasures(rawMeasures);
        jaceEvent.setRawTags(rawTags);

        return jaceEvent;
        // @formatter: on
    }

    public Double calculateKwhValue(Double currValue, PointDTO pointDTO)
    {
        // Find out the delta of the value in the PointDTO timePeriod.
        List<PointReadingDTO> histReadings = pointDTO.getPointReadings();

        // if we don't have the historical readings, we can't calculate the kwh.
        if (histReadings == null) {
            return null;
        }

        Long min_eventTs = Long.MAX_VALUE;
        Map<Long, Double> timeValueMap = new HashMap();

        // find the oldest value in the histReadings.
        for(PointReadingDTO reading: histReadings) {
//            logger.error("Readings = " + reading);
            if (reading.getStatus().equalsIgnoreCase("ok")) {
                long eventTs = getEventTimestamp(reading.getTimeStamp());
                if (reading.getValue() != null) {
                    timeValueMap.put(eventTs, Double.valueOf((String)reading.getValue()));
                }
                min_eventTs = Math.min(min_eventTs, eventTs);
            }
        }
        if (timeValueMap != null && timeValueMap.containsKey(min_eventTs)) {
            double prevValue = timeValueMap.get(min_eventTs);
            System.out.println("=== currValue = " + currValue + "  prevValue = " + prevValue);
            return Math.abs(currValue - prevValue);
        } else {
            return null;
        }
    }

    public Map<String, Double> calculateTagRuntime(String tagName, String currValue, 
            PointDTO pointDTO, int readingInterval, String currTime, Map<String, String> lastRuntime)
    {
        System.out.println("******** Calculate TAG RUNTIME **********");
        // Find out the delta of the value in the PointDTO timePeriod.
        List<PointReadingDTO> histReadings = pointDTO.getPointReadings();
        Map<String, Double> runtimeTags = new HashMap();

        String runtimeTagName = tagName + "_" + currValue.toLowerCase() + "_runtime";
        System.out.println("in TAG: runtimeTag = " + runtimeTagName + "  interval = " + readingInterval);
        String tagCntName = tagName + "_" + currValue.toLowerCase();
        System.out.println("=== latest tagCntName = " + tagCntName + " set count = 1");
        runtimeTags.put(tagCntName, 1.0);

        // if we don't have the historical readings, we will just set the runtime to the whole interval.
        if (histReadings == null) {
            runtimeTags.put(runtimeTagName, Double.valueOf(readingInterval));
            lastRuntime.put(runtimeTagName, String.valueOf(readingInterval));
            return runtimeTags;
        }

        Long min_eventTs = Long.MAX_VALUE;
        Map<Long, String> timeValueMap = new HashMap();
        Long currEventTs = getEventTimestamp(currTime);
        timeValueMap.put(currEventTs, currValue);

        // fill the map with all the historical values .
        for(PointReadingDTO reading: histReadings) {
            if (reading.getStatus().equalsIgnoreCase("ok")) {
                long eventTs = getEventTimestamp(reading.getTimeStamp());
                if (reading.getValue() != null) {
                    timeValueMap.put(eventTs, (String)reading.getValue());
                }
                min_eventTs = Math.min(min_eventTs, eventTs);
            }
        }

        // if nothing in the histReadings, just return the current status. 
        if (min_eventTs == Long.MAX_VALUE) {
            runtimeTags.put(runtimeTagName, Double.valueOf(readingInterval));
            lastRuntime.put(runtimeTagName, String.valueOf(readingInterval));
            return runtimeTags;
        }

        // get the initial state.  
        String lastState = timeValueMap.get(min_eventTs); 
        String latestTagName = null;
        long latestTagRuntime = 0L;
       
        long lastEventTs = (long) min_eventTs;
        double baseRuntime = 0;
        timeValueMap.remove(lastEventTs);

        // let's sort by the timestamp.
        TreeMap<Long, String> sorted = new TreeMap(timeValueMap);

        for(Long eventTs: sorted.keySet()) { 
            // calculate the run time difference.  
            long runtime = (eventTs - lastEventTs)/1000;
            runtimeTagName = tagName + "_" + lastState.toLowerCase() + "_runtime";  
            System.out.println("=== runTagName = " + runtimeTagName + " == runtime "+ runtime);
            tagCntName = tagName + "_" + lastState.toLowerCase();

            // calculate total runtime
            if (runtimeTags.containsKey(runtimeTagName)) {
                baseRuntime = runtimeTags.get(runtimeTagName);
            } else {
                baseRuntime = 0;
            }

            // update the total runtime. 
            runtimeTags.put(runtimeTagName, baseRuntime+runtime);
            System.out.println("=== runTagName = " + runtimeTagName 
                    +  " baseRuntime = " + baseRuntime + " add runtime = " + runtime);

            // calculate total count of this status. 
            if (runtimeTags.containsKey(tagCntName)) {
                double currCnt = runtimeTags.get(tagCntName);
                runtimeTags.put(tagCntName, currCnt+1);
            } else {
                System.out.println("**** first time for " + tagCntName + " cnt = 1");
                runtimeTags.put(tagCntName, 1.0);
            }
            System.out.println("=== tagCntName = " + tagCntName 
                     + " count = " + runtimeTags.get(tagCntName));

            if (latestTagName != null && latestTagName.equals(runtimeTagName)) { ;
               latestTagRuntime += runtime; 
            } else {
                // reset latest runtime if a new state comes. 
                latestTagName = runtimeTagName;
                latestTagRuntime = runtime;
            }

            System.out.println("=== latestTagName = " + latestTagName 
                     + " latestTagRuntime = " + runtime);

            // capture the last run time
            if (eventTs == currEventTs) {
                String lastRuntimeTag = tagName + "_" + lastState.toLowerCase() + "_runtime";
                //lastRuntime.put(lastRuntimeTag, String.valueOf(runtime));
                lastRuntime.put(lastRuntimeTag, String.valueOf(latestTagRuntime));
                System.out.println("==== Save in lastRuntime: lastTagName = " + lastRuntimeTag
                         + " .. latestTagRuntime = " + latestTagRuntime); 
            }

            // update the last values. 
            lastEventTs = eventTs;
            lastState = sorted.get(eventTs);

            System.out.println("==== RUNTIME TAGS =  "+ runtimeTags);
        }

        System.out.println("==== Total RUNTIME TAGS =  "+ runtimeTags);
        System.out.println("==== Latest RUNTIME Cnt TAGS =  "+ lastRuntime);
        return runtimeTags;
    }

    public Long getEventTimestamp(String timeString)  {
        if(timeString == null) return null;
        return DateUtils.parseTime(timeString, NormalizationConstants.JACE_TIME_FORMAT).toInstant().toEpochMilli();
    }
}
