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
 * Created by 212577826 on 1/18/17.
 */
public class ShortCyclingThermostatStateTest extends RuleBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShortCyclingThermostatStateTest.class);

    @Before
    public void init() {
        initRules("ShortCyclingThermostatState.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test public void itShouldDetectFault() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setTagsMap(MockObjectFactory.createTagsMapWithDetectFault());
      
        IERulesUtil.initRulesEngine(rules); List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOGGER.error("ERROR triggering rule: {}", e);
        }

        LOGGER.info("Condition met : {}", rulesBaseFact.getConditionMet());
        assertTrue(rulesBaseFact.getConditionMet());
    }

    @Test public void itShouldDetectNoFault() {

        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setTagsMap(MockObjectFactory.createTagsMapWithNoDetectFault());
        IERulesUtil.initRulesEngine(rules);
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {

            LOGGER.error("ERROR triggering rule: {}", e);
        }
        LOGGER.info("Condition met : {}", rulesBaseFact.getConditionMet());
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
            rulesBaseFact.setParameters(createParameters());
            rulesBaseFact.setTagsMap(tagsMap);
            rulesBaseFact.setConditionMet(false);
            return rulesBaseFact;
        }

        public static Map<String, Object> createMeasure(String tag, Object value) {
            return new HashMap<String, Object>() {{
                put(tag, value);
            }};
        }

        public static Map<String, Object> createTags(String tag, Object value) {
            return new HashMap<String, Object>() {{
                put(tag, value);
            }};
        }

        public static List<Map<String, Object>> createMeasuresMapWithDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.put(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 70.0);

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresMapWithNoDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.put(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 70.0);

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createTagsMapWithDetectFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();

            Map<String, Object> currenTagsMap1 = new HashMap<>();
            currenTagsMap1.putAll(createTags("heatStage1Cmd", "off"));
            currenTagsMap1.putAll(createTags("coolStage1Cmd", "off"));
            currenTagsMap1.putAll(createTags("event_ts", "123455"));

            Map<String, Object> currenTagsMap2 = new HashMap<>();
            currenTagsMap2.putAll(createTags("heatStage1Cmd", "on"));
            currenTagsMap2.putAll(createTags("coolStage1Cmd", "on"));
            currenTagsMap2.putAll(createTags("event_ts", "123455"));


            Map<String, Object> currenTagsMap3 = new HashMap<>();
            currenTagsMap3.putAll(createTags("heatStage1Cmd", "off"));
            currenTagsMap3.putAll(createTags("coolStage1Cmd", "off"));
            currenTagsMap3.putAll(createTags("event_ts", "123455"));

            tagsMap.add(currenTagsMap1);
            tagsMap.add(currenTagsMap2);
            tagsMap.add(currenTagsMap3);
            return tagsMap;
        }

        public static List<Map<String, Object>> createTagsMapWithNoDetectFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();

            for (int i = 0; i < 4; i++) {
                Map<String, Object> currenTagsMap = new HashMap<>();
                currenTagsMap.putAll(createTags("heatStage1Cmd", "on"));
                currenTagsMap.putAll(createTags("coolStage2Cmd", "on"));
                currenTagsMap.putAll(createTags("heatStage3Cmd", "off"));
                currenTagsMap.putAll(createTags("heatStage4Cmd", "off"));
                currenTagsMap.putAll(createTags("coolStage1Cmd", "on"));
                currenTagsMap.putAll(createTags("coolStage2Cmd", "off"));
                currenTagsMap.putAll(createTags("coolStage3Cmd", "off"));
                currenTagsMap.putAll(createTags("coolStage4Cmd", "off"));

                tagsMap.add(currenTagsMap);
            }

            return tagsMap;
        }

        public static Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("user_Threshold" + RulesUtil.getFDSINumber(RulesUtil.SHORT_CYCLING_THERMOSTAT_STATE), 1.0);
            return parameters;
        }
    }
}
