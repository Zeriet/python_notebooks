package com.ge.current.em.custom.analytics;

import com.ge.current.em.util.TagConstants;
import org.apache.commons.collections.map.HashedMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//This is temporary class until we can come up with the rules data model queries to get this data

public class TestMapUtil {


    public static Map<String, List<String>> getRuleRequiredMeasuresMapToBroadCast() {
        Map<String, List<String>> ruleMeasuresMap = new HashMap<>();

        List<String> requiredMeasures = new ArrayList<>();
        requiredMeasures.add(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName());
        requiredMeasures.add("zoneAirTempOccCoolingSp");
        ruleMeasuresMap.put("RULE-UID-01", requiredMeasures);
        ruleMeasuresMap.put("RULE-UID-02", requiredMeasures);

        requiredMeasures = new ArrayList<>();
        requiredMeasures.add(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName());
        requiredMeasures.add("zoneAirTempEffectiveSp");
        requiredMeasures.add("zoneAirTempOccCoolingSp");
        ruleMeasuresMap.put("RULE-UID-20", requiredMeasures);
        ruleMeasuresMap.put("RULE-UID-21", requiredMeasures);

        requiredMeasures = new ArrayList<>();
        requiredMeasures.add("dischargeAirTempSensor");
        requiredMeasures.add("ahuHeatStage1_on_runtime");
        requiredMeasures.add("ahuCoolStage1_on_runtime");
        requiredMeasures.add("zoneAirTempOccCoolingSp");
        requiredMeasures.add(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName());
        requiredMeasures.add("zoneAirTempEffectiveSp");
        requiredMeasures.add("currentSensorValue");

        requiredMeasures = new ArrayList<>();
        requiredMeasures.add(TagConstants.Measures.ZONE_AIR_TEMP_SENSOR.getMeasureName());
        requiredMeasures.add("zoneAirTempEffectiveSp");
        requiredMeasures.add("dischargeAirTempSensor");
        requiredMeasures.add("ahuHeatStage1_on_runtime");
        requiredMeasures.add("ahuCoolStage1_on_runtime");
        ruleMeasuresMap.put("RULE-UID-09", requiredMeasures);
        ruleMeasuresMap.put("RULE-UID-12", requiredMeasures);

        List<String> requiredMeasuresExcessiveLighting = new ArrayList<>();
        requiredMeasuresExcessiveLighting.add("indoorLightingStatusOnRuntime");

        List<String> requiredMeasuresExcessiveHVAC = new ArrayList<>();
        requiredMeasuresExcessiveHVAC.add("rtuHeatStage1Runtime");
        requiredMeasuresExcessiveHVAC.add("rtuCoolStage1Runtime");

        ruleMeasuresMap.put("RULE-UID-03", requiredMeasures);
        ruleMeasuresMap.put("RULE-UID-04", requiredMeasures);
        ruleMeasuresMap.put("RULE-UID-05", new ArrayList<>()); // no measures required
        ruleMeasuresMap.put("RULE-UID-06", new ArrayList<>()); // no measures required

        ruleMeasuresMap.put("RULE-UID-22", requiredMeasuresExcessiveHVAC);
        ruleMeasuresMap.put("RULE-UID-23", requiredMeasuresExcessiveLighting);

        List<String> requiredMeasuresExcessiveHVACTime = new ArrayList<>();
        requiredMeasuresExcessiveHVACTime.add("hvacStateOverride_true_runtime");
        ruleMeasuresMap.put("RULE-UID-07", requiredMeasuresExcessiveHVACTime);

        return ruleMeasuresMap;
    }

    public static Map<String, List<String>> getRuleRequiredTagsMapToBroadCast() {
        Map<String, List<String>> requiredTags = new HashMap<>();
        List<String> list = new ArrayList<>();
        requiredTags.put("RULE-UID-01", list);
        requiredTags.put("RULE-UID-02", list);
        requiredTags.put("RULE-UID-20", list);
        requiredTags.put("RULE-UID-21", list);

        List<String> listAhuOnOff = new ArrayList<>();
        listAhuOnOff.add("ahuCoolStage1");
        listAhuOnOff.add("ahuHeatStage1");
        listAhuOnOff.add("ahuCoolStage2");
        listAhuOnOff.add("ahuHeatStage2");
        listAhuOnOff.add("ahuCoolStage3");
        listAhuOnOff.add("ahuHeatStage3");
        listAhuOnOff.add("ahuCoolStage4");
        listAhuOnOff.add("ahuHeatStage4");

        List<String> listhvacStateOverride = new ArrayList<>();
        listhvacStateOverride.add("hvacStateOverride");

        List<String> listLightingStateOverride = new ArrayList<>();
        listLightingStateOverride.add("lightingStateOverride");
        requiredTags.put("RULE-UID-05", listhvacStateOverride);
        requiredTags.put("RULE-UID-06", listLightingStateOverride);

        List<String> listProvingFan = new ArrayList<>();
        listProvingFan.add("rtuFanStateLastStatus");

        list = new ArrayList<>();
        list.add("ahuCoolStage1");
        requiredTags.put("RULE-UID-09", list);
        requiredTags.put("RULE-UID-12", list);

        requiredTags.put("RULE-UID-03", listProvingFan);
        requiredTags.put("RULE-UID-04", listAhuOnOff);

        requiredTags.put("RULE-UID-22", new ArrayList<>());
        requiredTags.put("RULE-UID-23", new ArrayList<>());
        requiredTags.put("RULE-UID-07", new ArrayList<>());

        return requiredTags;
    }

    public static Map<String, List<String>> getRuleRequiredAssetType() {
        Map<String, List<String>> ruleRequiredAssetType = new HashMap<>();
        ruleRequiredAssetType.put("RULE-PROP-UID-02", Arrays.asList("rtu", "ahu"));
        return  ruleRequiredAssetType;
    }

    public static  Map<String, Map<String, Object>> getParametrsMapToBroadcast(){
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("userThreshold", "20");
        paramMap.put("comfortThreshold", "79");

        Map<String, Map<String, Object>> parametersMap = new HashMap<>();
        parametersMap.put("RULE-UID-01", paramMap);
        parametersMap.put("RULE-UID-02", paramMap);
        parametersMap.put("RULE-UID-03", paramMap);
        parametersMap.put("RULE-UID-03", paramMap);
        parametersMap.put("RULE-UID-05", paramMap);
        parametersMap.put("RULE-UID-06", paramMap);
        parametersMap.put("RULE-PROP-UID-02", paramMap);

        return parametersMap;
    }

    public static Map<String, String> getRuleNames(){
        Map<String, String> ruleNamesMap = new HashMap<>();
        ruleNamesMap.put("RULE-UID-01", "Too hot threshold");
        ruleNamesMap.put("RULE-UID-02", "Too cold threshold");
        ruleNamesMap.put("RULE-UID-03", "Proving Fan");
        ruleNamesMap.put("RULE-UID-04", "Cooling does not work");
        ruleNamesMap.put("RULE-UID-05", "Heating does not work");
        ruleNamesMap.put("RULE-UID-06", "Zone temperature too high");
        return ruleNamesMap;
    }

    public static Map<String, String> getRuleRegistryNames(){
        Map<String, String> ruleRegistry = new HashMap<>();
        ruleRegistry.put("Too hot threshold", "enterprise_GE");
        ruleRegistry.put("Too cold threshold", "enterprise_GE");
        ruleRegistry.put("Proving Fan", "enterprise_GE");
        ruleRegistry.put("Cooling does not work", "enterprise_GE");
        ruleRegistry.put("Heating does not work", "enterprise_GE");
        ruleRegistry.put("Zone temperature too high", "enterprise_GE");
        return ruleRegistry;
    }
}
