package com.ge.current.em.rules;

import com.ge.current.em.custom.analytics.drools.IERulesUtil;
import com.ge.current.em.entities.analytics.AlarmObject;
import com.ge.current.em.entities.analytics.RulesBaseFact;
import com.ge.current.em.util.RulesUtil;
import com.ge.current.em.util.TagConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by 212554696 on 1/10/17.
 */
public class TooColdThresholdTest extends RuleBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(TooColdThresholdTest.class);

    @Before
    public void init() {
        initRules("TooHotThreshold.drl", "TooColdThreshold.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test
    public void itShouldMeetConditionIfFaultIsDetected() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(MockObjectFactory.createFaultMeasuresMap());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        assertTrue(rulesBaseFact.getConditionMet());
    }

    @Test
    public void itShouldNotMeetConditionIfMeasuresAreInvalid() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(MockObjectFactory.createInvalidMeasureMapWithNull());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        assertFalse(rulesBaseFact.getConditionMet());
    }

    @Test
    public void itShouldNotMeetConditionIfFaultIsNotDetected() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(MockObjectFactory.createNonFaultMeasureMap());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        assertFalse(rulesBaseFact.getConditionMet());
    }

    private static class MockObjectFactory {

        public static RulesBaseFact getFacts() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            String assetId = "ASSET_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
            String segmentId = "SEGMENT_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
            String enterpriseId = "ENTERPRISE_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
            String siteId = "SITE_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
            RulesBaseFact rulesBaseFact = new RulesBaseFact();
            rulesBaseFact.setAssetId(assetId);
            rulesBaseFact.setSegmentId(segmentId);
            rulesBaseFact.setEnterpriseId(enterpriseId);
            rulesBaseFact.setSiteId(siteId);
            rulesBaseFact.setAlarmObject(new AlarmObject());
            rulesBaseFact.setMeasuresAvgMap(null);
            rulesBaseFact.setParameters(createParameters());
            rulesBaseFact.setTagsMap(tagsMap);
            rulesBaseFact.setConditionMet(false);
            rulesBaseFact.setAlarmObject(new AlarmObject());
            return rulesBaseFact;
        }

        public static Map<String, Object> createMap(String tag, Object value) {
            return new HashMap<String, Object>() {{
                put(tag, value);
            }};
        }


        public static Map<String, Object> createTags(String tag, Object value) {
            return new HashMap<String, Object>() {{
                put(tag, value);
            }};
        }

        public static List<Map<String, Object>> createFaultMeasuresMap() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            Calendar date = Calendar.getInstance();
            long now = date.getTimeInMillis();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                Random rand = new Random();
                int zoneAirTempSensor = rand.nextInt(5) + 59;
                Date timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentMeasures.putAll(createMap(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), new Double(zoneAirTempSensor)));
                currentMeasures.putAll(createMap(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createNonFaultMeasureMap() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            Calendar date = Calendar.getInstance();
            long now = date.getTimeInMillis();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                Random rand = new Random();
                int zoneAirTempSensor = rand.nextInt(5) + 65;
                Date timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentMeasures.putAll(createMap(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), new Double(zoneAirTempSensor)));
                currentMeasures.putAll(createMap(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createInvalidMeasureMapWithNull() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMap(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), null));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.putAll(createMap(TagConstants.Parameters.COMFORT_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.TOO_COLD_THRESHOLD), 65.0));
            parameters.putAll(createMap(TagConstants.Parameters.DURATION.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.TOO_COLD_THRESHOLD), 15.0));
            parameters.putAll(createMap(TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.TOO_COLD_THRESHOLD), 1.0));
            return parameters;
        }
    }
}
