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
 * Created by 212577826 on 1/20/17.
 */
public class ExcessiveHVACOverridesCountTest extends RuleBaseTest {

    private static Logger LOGGER = LoggerFactory.getLogger(ExcessiveHVACOverridesCountTest.class);

    @Before
    public void init() {
        initRules("ExcessiveLightingOverridesCount.drl", "ExcessiveHVACOverridesCount.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test public void itShouldDetectFault() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(MockObjectFactory.createMeasuresMapWithDetectFault());
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
    public  void itShouldDetectNoFault(){

        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAggrMap(MockObjectFactory.createMeasuresWithNoDetectFault());
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
            rulesBaseFact.setMeasuresAggrMap(null);
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
                currentMeasures.putAll(createTags(TagConstants.Tags.HVAC_STATE_OVERRIDE.getTagName(), 12.0));
                currentMeasures.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(), "1234"));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }


        public static List<Map<String, Object>> createTagsMapWithDetectFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            for (int i = 0; i < 12; i++) {
                Map<String, Object> currenTagsMap = new HashMap<>();
                currenTagsMap.putAll(createTags(TagConstants.Tags.HVAC_STATE_OVERRIDE.getTagName(), 12.0));
                currenTagsMap.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(), "1234"));
                tagsMap.add(currenTagsMap);
            }

            return tagsMap;
        }

        public static List<Map<String, Object>> createMeasuresWithNoDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createTags(TagConstants.Tags.HVAC_STATE_OVERRIDE.getTagName(), 1.0));
                currentMeasures.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(), "1234"));

                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }


        public static Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put(TagConstants.Parameters.USER_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.EXCESSIVE_HVAC_OVERRIDES_COUNT), 10.0d);
            return parameters;
        }
    }
}
