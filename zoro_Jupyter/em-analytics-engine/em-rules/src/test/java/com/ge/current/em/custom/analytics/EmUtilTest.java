package com.ge.current.em.custom.analytics;

import com.ge.current.em.util.EmUtil;
//import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by 212577826 on 2/16/17.
 */
public class EmUtilTest {

    private static final Logger LOG = LoggerFactory.getLogger(EmUtilTest.class);

    @Test public void itShoutdSortTheList() {
        String eventTs = "event_ts";
        List<Map<String, Object>> unsortedMap = getUnsortedMapEventTs();
        List<Map<String, Object>> expectedOutPut = getSortedMapEventTs();
        List<Map<String, Object>> actualOutPut = EmUtil.sortListOfMapsBasedOnKey(unsortedMap, "event_ts");
        for (int i = 0; i < actualOutPut.size(); i++) {
            String actualEventTs = actualOutPut.get(i).get(eventTs).toString();
            String expectedEventTs = expectedOutPut.get(i).get(eventTs).toString();
            System.out.println(" == actual event_ts: " + actualEventTs + "  expected events Ts:" + expectedEventTs);
            assertEquals(expectedEventTs, actualEventTs);
        }
    }

    @Test
    public void itShouldReturnCorrectTimeFormat(){

        String yyyyMMddFormat = "yyyyMMdd";
        String HHmmFormat  = "HHmm";
        String timeZoneLocal = "America/Los_Angeles";
        String utcTIme = "1473948000000";  //  2016-09-15 07:00
        String expectedLocalDate = "20160915";
        String expectedTime = "0700";
        String actualLocalDate = EmUtil.convertUTCStringTimeToLocalTime(utcTIme, timeZoneLocal,yyyyMMddFormat);
        String actualTime =  EmUtil.convertUTCStringTimeToLocalTime(utcTIme, timeZoneLocal, HHmmFormat);
        System.out.println(" actual local date: " + actualLocalDate + " actual time: " + actualTime);
        assertTrue(expectedLocalDate.equals(actualLocalDate));
        assertTrue(expectedTime.equals(actualTime));
    }


    public List<Map<String, Object>> getUnsortedMapEventTs() {

        List<Map<String, Object>> unSortedMap = new ArrayList<>();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("event_ts", "201701121000");
        map1.put("zoneAirTempSensor", 70.2);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("event_ts", "201711122215");
        map2.put("zoneAirTempSensor", 70.2);

        Map<String, Object> map3 = new HashMap<>();
        map3.put("event_ts", "201705121020");
        map3.put("zoneAirTempSensor", 70.2);

        Map<String, Object> map4 = new HashMap<>();
        map4.put("event_ts", "201701120900");
        map4.put("zoneAirTempSensor", 70.2);

        unSortedMap.add(map1);
        unSortedMap.add(map2);
        unSortedMap.add(map3);
        unSortedMap.add(map4);

        return unSortedMap;
    }

    public List<Map<String, Object>> getSortedMapEventTs() {
        List<Map<String, Object>> list = getUnsortedMapEventTs();
        // the map list when sorted in event_ts should be ordered as map4, map1, map3, map2
        Map<String, Object> map1 = list.remove(0);
        Map<String, Object> map2 = list.remove(0);
        Map<String, Object> map3 = list.remove(0);
        Map<String, Object> map4 = list.remove(0);
        list.add(map4);
        list.add(map1);
        list.add(map3);
        list.add(map2);
        return list;
    }
}