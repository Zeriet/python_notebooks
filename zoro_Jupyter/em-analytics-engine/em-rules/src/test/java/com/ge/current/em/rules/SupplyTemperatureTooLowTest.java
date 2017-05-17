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
 * Created by 212554696 on 5/1/17.
 */
public class SupplyTemperatureTooLowTest extends RuleBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(SupplyTemperatureTooLowTest.class);

    private static final double DISCHARGE_AIR_TEMP_FAULT_VALUE = 59;
    private static final double TEMPERATURE_THRESHOLD = 66;
    private static final double TEMP_HOLD_DURATION = 60;
    private static final double DEFAULT_SEVERITY = 2.0;
    
    @Before
    public void init() {
        initRules("SupplyTemperatureTooHigh.drl", "SupplyTemperatureTooLow.drl");
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
                int dischargeAirTempSensor = rand.nextInt(5) + (int) DISCHARGE_AIR_TEMP_FAULT_VALUE;
                Date timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentMeasures.putAll(createMap(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(),
                        new Double(dischargeAirTempSensor)));
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
                int dischargeAirTempSensor = rand.nextInt(5) + (int) TEMPERATURE_THRESHOLD;
                Date timestamp = new Date(now + (15 * i * ONE_MINUTE_IN_MILLIS));
                currentMeasures.putAll(createMap(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(),
                        new Double(dischargeAirTempSensor)));
                currentMeasures.putAll(createMap(TagConstants.Measures.EVENT_TS.getMeasureName(), timestamp));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public List<Map<String, Object>> createInvalidMeasureMapWithNull() {
            List<Map<String, Object>> measuresMap = new LinkedList<>();
            for (int i = 0; i < 4; i++) {
                Map<String, Object> currentMeasures = new LinkedHashMap<>();
                currentMeasures.putAll(createMap(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), null));
                measuresMap.add(currentMeasures);
            }
            return measuresMap;
        }

        public Map<String, Object> createParameters() {
            Map<String, Object> parameters = new HashMap<>();
            parameters.putAll(createMap(TagConstants.Parameters.TEMPERATURE_THRESHOLD.getParameterName() + RulesUtil
                    .getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW), TEMPERATURE_THRESHOLD));
            parameters.putAll(createMap(TagConstants.Parameters.TEMP_HOLD_DURATION.getParameterName() + RulesUtil
                    .getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW), TEMP_HOLD_DURATION));
            parameters.putAll(createMap(TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil
                    .getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW), DEFAULT_SEVERITY));
            return parameters;
        }
    }
}
