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
public class CoolingDoesNotWorkTest extends RuleBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(CoolingDoesNotWorkTest.class);

    @Before
    public void init() {
        initRules("CoolingDoesNotWork.drl", "HeatingDoesNotWork.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test public void itShouldDtectFault() {
        RulesBaseFact rulesBaseFact = CoolingDoesNotWorkTest.MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(CoolingDoesNotWorkTest.MockObjectFactory.createMeasuresAggMapWithDetectFault());
        rulesBaseFact.setMeasuresAvgMap(CoolingDoesNotWorkTest.MockObjectFactory.createMeasuresAvgMapWithDetectFault());
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
    
    @Test public void itShouldNotDetectFaultNotQualifing() {
        RulesBaseFact rulesBaseFact = CoolingDoesNotWorkTest.MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(CoolingDoesNotWorkTest.MockObjectFactory.createMeasuresAvgMapWithNoQualifyingPeriodFault());
        rulesBaseFact.setMeasuresAvgMap(CoolingDoesNotWorkTest.MockObjectFactory.createMeasuresAggrMapWithNoQualifyingPeriodFault());
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

    @Test public void itShouldReturnCorrectDuration() {
        RulesBaseFact rulesBaseFact = CoolingDoesNotWorkTest.MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(CoolingDoesNotWorkTest.MockObjectFactory.createMeasuresAggMapWithDetectFault());
        rulesBaseFact.setMeasuresAvgMap(CoolingDoesNotWorkTest.MockObjectFactory.createMeasuresAvgMapWithDetectFault());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        IERulesUtil.initRulesEngine(rules);

        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        LOG.info("Condition met: {}", rulesBaseFact.getConditionMet());
        assertEquals(rulesBaseFact.getAlarmObject().getDuration(), 60.00, 0.01);
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

        public static List<Map<String, Object>> createMeasuresAvgMapWithDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 70.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), 60.0));
                currentMeasures.putAll(createMeasure(TagConstants.Measures.EVENT_TS.getMeasureName(), "1234"));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresAggMapWithDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_COOL_STAGE_1_ON_RUNTIME.getTagName(), 16.0*60));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_HEAT_STAGE_1_ON_RUNTIME.getTagName(), 16.0*60));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresAvgMapWithNoQualifyingPeriodFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                // create one qualified entry
                currentMeasures.putAll(createMeasure(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 68.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), 70.0));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresAggrMapWithNoQualifyingPeriodFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                // create one qualified entry
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_COOL_STAGE_1_ON_RUNTIME.getTagName(), 16.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_HEAT_STAGE_1_ON_RUNTIME.getTagName(), 16.0));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }
        
        
        public static List<Map<String, Object>> createMeasuresAvgMapWithNoDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                // create one qualified entry
                currentMeasures.putAll(createMeasure(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName(), 68.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), 70.0));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresAggrMapWithNoDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                // create one qualified entry
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_COOL_STAGE_1_ON_RUNTIME.getTagName(), 1.0));
                currentMeasures.putAll(createMeasure(TagConstants.Tags.AHU_HEAT_STAGE_1_ON_RUNTIME.getTagName(), 1.0));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }
        
        public static Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.putAll(createMeasure(TagConstants.Parameters.MIN_ITD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.COOLING_DOES_NOT_WORK), -11.0));
            parameters.putAll(createMeasure(TagConstants.Parameters.RUNTIME_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.COOLING_DOES_NOT_WORK), 15.0));

            return parameters;
        }
    }
}

