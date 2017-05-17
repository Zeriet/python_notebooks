package com.ge.current.em.rules;

import com.ge.current.em.custom.analytics.drools.IERulesUtil;
import com.ge.current.em.entities.analytics.RulesBaseFact;
import com.ge.current.em.util.RulesUtil;
import com.ge.current.em.util.TagConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by 212577826 on 1/18/17.
 */
public class SimultaneousHeatingAndCoolingTest extends RuleBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimultaneousHeatingAndCoolingTest.class);

    private static final double DEFAULT_SEVERITY = 3.0;

    @Before
    public void init() {
        initRules("SimultaneousHeatingAndCooling.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test
    public void itShouldMeetConditionIfFaultIsDetected() {
        MockObjectFactory mockObjectFactory = new MockObjectFactory();
        RulesBaseFact rulesBaseFact = mockObjectFactory.getFacts();
        rulesBaseFact.setTagsMap(mockObjectFactory.createTagsMapWithFault());
      
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
    public void itShouldNotMeetConditionIfThereIsNoFaultDetected() {
        MockObjectFactory mockObjectFactory = new MockObjectFactory();
        RulesBaseFact rulesBaseFact = mockObjectFactory.getFacts();
        rulesBaseFact.setTagsMap(mockObjectFactory.createTagsMapWithNonFault());
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

    @Test
    public void itShouldNotMeetConditionIfAlertTsIsNull() {
        MockObjectFactory mockObjectFactory = new MockObjectFactory();
        RulesBaseFact rulesBaseFact = mockObjectFactory.getFacts();
        rulesBaseFact.setTagsMap(mockObjectFactory.createTagsMapWithNullAlertTs());
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

    private static class MockObjectFactory extends GlobalMockObjectFactory {

        public List<Map<String, Object>> createTagsMapWithFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            Calendar date = Calendar.getInstance();
            long now = date.getTimeInMillis();
            Date timestamp;
            Map<String, Object> currentTagsMap;
            for (int i = 0; i < 3; i++) {
                timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentTagsMap = new HashMap<>();
                currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_HEAT_STAGE_1.getTagName(), TagConstants.ON_COMMAND_STATUS));
                currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_COOL_STAGE_1.getTagName(), TagConstants.OFF_COMMAND_STATUS));
                currentTagsMap.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
                tagsMap.add(currentTagsMap);
            }
            currentTagsMap = new HashMap<>();
            currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_HEAT_STAGE_1.getTagName(), TagConstants.ON_COMMAND_STATUS));
            currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_COOL_STAGE_1.getTagName(), TagConstants.ON_COMMAND_STATUS));
            timestamp = new Date(now + (15 * 4 * ONE_MINUTE_IN_MILLIS));
            currentTagsMap.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
            tagsMap.add(currentTagsMap);
            return tagsMap;
        }

        public List<Map<String, Object>> createTagsMapWithNonFault() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            Calendar date = Calendar.getInstance();
            long now = date.getTimeInMillis();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentTagsMap = new HashMap<>();
                Date timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_HEAT_STAGE_1.getTagName(), TagConstants.ON_COMMAND_STATUS));
                currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_COOL_STAGE_1.getTagName(), TagConstants.OFF_COMMAND_STATUS));
                currentTagsMap.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
                tagsMap.add(currentTagsMap);
            }
            return tagsMap;
        }

        public List<Map<String, Object>> createTagsMapWithNullAlertTs() {
            List<Map<String, Object>> tagsMap = new LinkedList<>();
            Map<String, Object> currentTagsMap;
            for (int i = 0; i < 3; i++) {
                currentTagsMap = new HashMap<>();
                currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_HEAT_STAGE_1.getTagName(), TagConstants.ON_COMMAND_STATUS));
                currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_COOL_STAGE_1.getTagName(), TagConstants.OFF_COMMAND_STATUS));
                currentTagsMap.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(),  null));
                tagsMap.add(currentTagsMap);
            }
            currentTagsMap = new HashMap<>();
            currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_HEAT_STAGE_1.getTagName(), TagConstants.OFF_COMMAND_STATUS));
            currentTagsMap.putAll(createTags(TagConstants.Tags.AHU_COOL_STAGE_1.getTagName(), TagConstants.ON_COMMAND_STATUS));
            currentTagsMap.putAll(createTags(TagConstants.Measures.EVENT_TS.getMeasureName(), null));
            tagsMap.add(currentTagsMap);
            return tagsMap;
        }

        public Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put( TagConstants.Parameters.SEVERITY.getParameterName()
                    + RulesUtil.getFDSINumber(RulesUtil.SIMULTANEOUS_HEATING_AND_COOLING), DEFAULT_SEVERITY);
            return parameters;
        }
    }
}
