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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by 212554696 on 4/28/17.
 */
public class ReturnTemperatureTooHighTest extends RuleBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReturnTemperatureTooHighTest.class);

    private static final double RETURN_AIR_TEMP_FAULT_VALUE = 76;
    private static final double TEMPERATURE_THRESHOLD = 75;
    private static final double TEMP_HOLD_DURATION = 60;
    private static final double DEFAULT_SEVERITY = 2.0;

    @Before
    public void init() {
        initRules("ReturnTemperatureTooHigh.drl", "ReturnTemperatureTooLow.drl");
    }

    @After
    public void shutdown() {
        resetKieSession();
    }

    @Test
    public void itShouldMeetConditionIfFaultIsDetected() {
        MockObjectFactory mockObjectFactory = new MockObjectFactory();
        RulesBaseFact rulesBaseFact = mockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(mockObjectFactory.createFaultMeasuresMap());
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
    public void itShouldNotMeetConditionIfThereIsNoFaultDetected() {
        MockObjectFactory mockObjectFactory = new MockObjectFactory();
        RulesBaseFact rulesBaseFact = mockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(mockObjectFactory.createNonFaultMeasureMap());
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
    public void itShouldNotMeetConditionIfOneOfTheMeasuresIsNull() {
        MockObjectFactory mockObjectFactory = new MockObjectFactory();
        RulesBaseFact rulesBaseFact = mockObjectFactory.getFacts();
        rulesBaseFact.setMeasuresAvgMap(mockObjectFactory.createInvalidMeasureMapWithNull());
        List<RulesBaseFact> list = new ArrayList<>();
        list.add(rulesBaseFact);
        try {
            IERulesUtil.applyRule(rulesBaseFact);
        } catch (Exception e) {
            LOG.error("ERROR triggering rule: {}", e);
        }
        assertFalse(rulesBaseFact.getConditionMet());

    }

    private static class MockObjectFactory extends GlobalMockObjectFactory {


        public List<Map<String, Object>> createFaultMeasuresMap() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            Calendar date = Calendar.getInstance();
            long now = date.getTimeInMillis();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new LinkedHashMap<>();
                Random rand = new Random();
                int returnAirTempSensor = rand.nextInt(5) + (int)RETURN_AIR_TEMP_FAULT_VALUE;
                Date timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentMeasures.putAll(createMap(TagConstants.Measures.RETURN_AIR_TEMP_SENSOR.getMeasureName(),
                        new Double(returnAirTempSensor)));
                currentMeasures.putAll(createMap(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public List<Map<String, Object>> createNonFaultMeasureMap() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            Calendar date = Calendar.getInstance();
            long now = date.getTimeInMillis();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new LinkedHashMap<>();
                Random rand = new Random();
                int returnAirTempSensor = (int) TEMPERATURE_THRESHOLD - rand.nextInt(5);
                Date timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentMeasures.putAll(createMap(TagConstants.Measures.RETURN_AIR_TEMP_SENSOR.getMeasureName(),
                        new Double(returnAirTempSensor)));
                currentMeasures.putAll(createMap(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public List<Map<String, Object>> createInvalidMeasureMapWithNull() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new LinkedHashMap<>();
                currentMeasures.putAll(createMap(TagConstants.Measures.RETURN_AIR_TEMP_SENSOR.getMeasureName(), null));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.putAll(createMap(TagConstants.Parameters.TEMPERATURE_THRESHOLD.getParameterName() + RulesUtil
                    .getFDSINumber(RulesUtil.RETURN_TEMPERATURE_TOO_HIGH), TEMPERATURE_THRESHOLD));
            parameters.putAll(createMap(TagConstants.Parameters.TEMP_HOLD_DURATION.getParameterName() + RulesUtil
                    .getFDSINumber(RulesUtil.RETURN_TEMPERATURE_TOO_HIGH), TEMP_HOLD_DURATION));
            parameters.putAll(createMap(TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil
                    .getFDSINumber(RulesUtil.RETURN_TEMPERATURE_TOO_HIGH), DEFAULT_SEVERITY));
            return parameters;
        }
    }
}