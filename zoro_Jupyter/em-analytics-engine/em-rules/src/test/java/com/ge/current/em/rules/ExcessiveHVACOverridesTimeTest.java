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
 * Created by 212577826 on 1/23/17.
 */
public class ExcessiveHVACOverridesTimeTest extends RuleBaseTest {

    private static Logger LOGGER = LoggerFactory.getLogger(ExcessiveHVACOverridesTimeTest.class);

    @Before
    public void init() {
        initRules("ExcessiveHVACOverridesTime.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test public void itShouldDetectFault() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(MockObjectFactory.createDetectFaultMeasuresMap());
        rulesBaseFact.setParameters(MockObjectFactory.createParameters());
        IERulesUtil.initRulesEngine(rules);
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOGGER.error("ERROR triggering rule: {}", e);
        }

        LOGGER.info("Condition met : {}", rulesBaseFact.getConditionMet());
        assertTrue(rulesBaseFact.getConditionMet());
    }

    @Test
    public void itShouldReturnCorrectDuration(){

        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(MockObjectFactory.createDetectFaultMeasuresMap());
        rulesBaseFact.setParameters(MockObjectFactory.createParameters());
        IERulesUtil.initRulesEngine(rules);
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOGGER.error("ERROR triggering rule: {}", e);
        }
        double expecteDuration  = 4 * 10; // number of ons from createDetectFaultMeasuresMap
        double actualDuration = rulesBaseFact.getAlarmObject().getDuration();
        LOGGER.info("Expected duration is: {}", rulesBaseFact.getAlarmObject().getDuration());

        assertEquals(expecteDuration, actualDuration, 0.001);
    }

    @Test
    public  void itShouldDetectNoFaultOverrideTimeLow(){

        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(MockObjectFactory.createNoDetectFaultOverrideTimeLowMeasuresMap());
        rulesBaseFact.setParameters(MockObjectFactory.createParameters());
        IERulesUtil.initRulesEngine(rules);
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try{
            IERulesUtil.applyRule(rulesBaseFact);
        }catch (Exception e) {

            LOGGER.error("ERROR triggering rule: {}", e);
        }
        LOGGER.info("Condition met : {}", rulesBaseFact.getConditionMet());
        assertFalse(rulesBaseFact.getConditionMet());

    }

    @Test
    public  void itShouldDetectNoFaultMeasureTagNotAvaileble(){

        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(MockObjectFactory.createNoDetectFaultMeasuresMapTagNotAvailable());
        rulesBaseFact.setParameters(MockObjectFactory.createParameters());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        IERulesUtil.initRulesEngine(rules);
        try{
            IERulesUtil.applyRule(rulesBaseFact);
        }catch (Exception e) {

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

        public static List<Map<String, Object>> createDetectFaultMeasuresMap() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.put(TagConstants.Tags.HVAC_STATE_OVERRIDE_TRUE_RUNTIME.getTagName(), 10.0 *60);
                currentMeasures.put(TagConstants.Measures.EVENT_TS.getMeasureName(),"12345");
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createNoDetectFaultOverrideTimeLowMeasuresMap() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 2; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.put(TagConstants.Tags.HVAC_STATE_OVERRIDE_TRUE_RUNTIME.getTagName(), 5.0);

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createNoDetectFaultMeasuresMapTagNotAvailable() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 2; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                // valid tag is hvacStateOverride_true_runtime"
                currentMeasures.put("hvacStateOverride_true", 5.0);

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put(TagConstants.Parameters.USER_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.EXCESSIVE_HVAC_OVERRIDES_TIME), 20.0);
            return parameters;
        }
    }
}
