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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
/**
 * Created by 212554696 on 1/11/17.
 */
public class ProvingFanTest extends RuleBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(ProvingFanTest.class);

    @Before
    public void init() {
        initRules("ProvingFan.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test public void testPositiveUseCase() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(MockObjectFactory.createMeasuresMapWithDetectFault());
        rulesBaseFact.setTagsMap(MockObjectFactory.createTagsMapWithFault());
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
        LOG.info("RESULT: {}", rulesBaseFact);
    }

    @Test public void itShouldNotMeetConditionIfThereIsNoFaultDetected() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(MockObjectFactory.createMeasuresMapWithoutFault());
        rulesBaseFact.setTagsMap(MockObjectFactory.createTagsMapWithoutFault());
        IERulesUtil.initRulesEngine(rules);
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

    @Test public void itShouldNotMeetConditionIfTagsAreInvalid() {
        RulesBaseFact rulesBaseFact = MockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(MockObjectFactory.createMeasuresMapWithDetectFault());
        rulesBaseFact.setTagsMap(MockObjectFactory.createInvalidTags());
        IERulesUtil.initRulesEngine(rules);
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
            rulesBaseFact.setMeasuresAvgMap(null);
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
        public static List<Map<String, Object>> createTagsMapWithFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currenTagsMap = new HashMap<>();
                currenTagsMap.putAll(createTags(TagConstants.Tags.DISCHARGE_AIR_FAN_CMD.getTagName(), "True"));
                tagsMap.add(currenTagsMap);
            }
            return tagsMap;
        }

        public static List<Map<String, Object>> createTagsMapWithoutFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            Map<String, Object> currenTagsMap = new HashMap<>();
            currenTagsMap.putAll(createTags(TagConstants.Tags.DISCHARGE_AIR_FAN_CMD.getTagName(), "FALSE"));
            tagsMap.add(currenTagsMap);
            return tagsMap;
        }

        public static List<Map<String, Object>> createInvalidTags() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            Map<String, Object> currenTagsMap = new HashMap<>();
            /* create tag without the FDSI number: this should not meet the condition */
            currenTagsMap.putAll(createTags(TagConstants.Tags.DISCHARGE_AIR_FAN_CMD.getTagName() + "invalid", "TRUE"));
            tagsMap.add(currenTagsMap);
            for (int i = 0; i < 3; i++) {
                currenTagsMap = new HashMap<>();
                currenTagsMap.putAll(createTags(TagConstants.Tags.DISCHARGE_AIR_FAN_CMD.getTagName() + "invalid", "TRUE"));
                tagsMap.add(currenTagsMap);
            }
            return tagsMap;
        }

        public static List<Map<String, Object>> createMeasuresMapWithDetectFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            Map<String, Object> currentMeasures = new HashMap<>();
            currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_FAN_CURRENT_SENSOR.getTagName(), 1.0));
            measuresMap.add(currentMeasures);
            for (int i = 0; i < 3; i++) {
                currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_FAN_CURRENT_SENSOR.getTagName(), 2.0));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static List<Map<String, Object>> createMeasuresMapWithoutFault() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new HashMap<>();
                currentMeasures.putAll(createMeasure(TagConstants.Tags.DISCHARGE_AIR_FAN_CURRENT_SENSOR.getTagName(), 2.0));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public static Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put(TagConstants.Parameters.CURRENT_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.PROVING_FAN), 2.0);
            parameters.putAll(createTags(TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil
                    .getFDSINumber(RulesUtil.PROVING_FAN), 1.0));
            return parameters;
        }
    }
}
