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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by 212577826 on 1/5/17.
 */
public class HeatingDoesNotWorkTest extends RuleBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(HeatingDoesNotWorkTest.class);

    @Before
    public void init() {
        initRules("CoolingDoesNotWork.drl", "HeatingDoesNotWork.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test public void itShouldDetectFault() {
        RulesBaseFact rulesBaseFact = HeatingDoesNotWorkTest.MockObjectFactory.getFacts();

        rulesBaseFact.setMeasuresAggrMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAggrWithDetectFault());
        rulesBaseFact.setMeasuresAvgMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAvgWithDetectFault());

        IERulesUtil.initRulesEngine(rules);
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        LOG.info("Condition met: {}", rulesBaseFact.getConditionMet());
        assertTrue(rulesBaseFact.getConditionMet());
    }

    @Test public void itShouldReturnCorrectDuration() {
        RulesBaseFact rulesBaseFact = HeatingDoesNotWorkTest.MockObjectFactory.getFacts();

        rulesBaseFact.setMeasuresAggrMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAggrWithDetectFault());
        rulesBaseFact.setMeasuresAvgMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAvgWithDetectFault());

        IERulesUtil.initRulesEngine(rules);
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        LOG.info("Condition met: {}", rulesBaseFact.getConditionMet());
        assertEquals(rulesBaseFact.getAlarmObject().getDuration(), 60.00, 0.01);
    }

    @Test public void itShouldNotDetectFaultLowSumOfAhuRtuTags() {
        RulesBaseFact rulesBaseFact = HeatingDoesNotWorkTest.MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAggrWithNoDetectFault());
        rulesBaseFact.setMeasuresAvgMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAvgWithDetectFault());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        LOG.info("Condition met: {}", rulesBaseFact.getConditionMet());
        assertFalse(rulesBaseFact.getConditionMet());
    }

    @Test public void itShouldNotDetectFaultWhenMinITDNotMet() {
        RulesBaseFact rulesBaseFact = HeatingDoesNotWorkTest.MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAggrWithDetectFault());
        rulesBaseFact.setMeasuresAvgMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapAvgWithNoDetectFault());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        LOG.info("Condition met: {}", rulesBaseFact.getConditionMet());
        assertFalse(rulesBaseFact.getConditionMet());
    }


    @Test public void itShouldNotDetectFaultMissingAhuRtuTags() {

        RulesBaseFact rulesBaseFact = HeatingDoesNotWorkTest.MockObjectFactory.getFacts();
//        rulesBaseFact.setMeasuresMap(HeatingDoesNotWorkTest.MockObjectFactory.createMeasuresMapWithNoDetectFaultMissingAhuAndRtu());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        LOG.info("Condition met: {}", rulesBaseFact.getConditionMet());
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
        public static List<Map<String, Object>> createTagsMapWithDetectFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currenTagsMap = new HashMap<>();
                currenTagsMap.putAll(createTags("ahuHeatStage1", "on"));
                currenTagsMap.putAll(createTags("ahuCoolStage1", "on"));
                tagsMap.add(currenTagsMap);
            }
            return tagsMap;
        }

        public static List<Map<String, Object>> createMeasuresMapAvgWithDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 60.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), 85.0));
                currentMeasures.putAll(createMeasure(TagConstants.Measures.EVENT_TS.getMeasureName(), "1234"));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresMapAvgWithNoDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 80.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), 70.0));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }


        public static List<Map<String, Object>> createMeasuresMapAggrWithDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_COOL_STAGE_1_ON_RUNTIME.getTagName(), 16.0*60));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_HEAT_STAGE_1_ON_RUNTIME.getTagName(), 16.0*60));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresMapAggrWithNoDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_COOL_STAGE_1_ON_RUNTIME.getTagName(), 1.0*60));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_HEAT_STAGE_1_ON_RUNTIME.getTagName(), 1.0*60));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }


        public static List<Map<String, Object>> createMeasuresMapWithNoDetectFaultMissingAhuAndRtu() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                // create one qualified entry
                currentMeasures.putAll(createMeasure(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 68.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.ZONE_AIR_TEMP_EFFECTIVE_SP.getTagName(), 68.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), 70.0));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.putAll(createMeasure(TagConstants.Parameters.MIN_ITD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.HEATING_DOES_NOT_WORK), -10.0));
            parameters.putAll(createMeasure(TagConstants.Parameters.RUNTIME_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.HEATING_DOES_NOT_WORK), 20.0));
            return parameters;
        }
    }
}
