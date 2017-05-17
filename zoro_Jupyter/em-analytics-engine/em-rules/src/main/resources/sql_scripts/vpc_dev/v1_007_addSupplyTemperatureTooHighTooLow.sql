/* rule definition */
INSERT INTO emsdev_emsrules.rule (rule_id,rule_uid,rule_name,rule_type,document_uid,source_rule_id,from_date,thru_date,description,authored_by,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-UID-24','Supply Temperature Too High',DEFAULT,'DOC-UID-24',(SELECT currval('emsdev_emsrules.rule_id_seq')),DEFAULT,DEFAULT,'Discharge temperature exceeds a user defined temperature threshold for at least a user defined duration.','team analytics',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule (rule_id,rule_uid,rule_name,rule_type,document_uid,source_rule_id,from_date,thru_date,description,authored_by,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-UID-25','Supply Temperature Too Low',DEFAULT,'DOC-UID-25',(SELECT currval('emsdev_emsrules.rule_id_seq')),DEFAULT,DEFAULT,'Discharge temperature cannot reach a user defined temperature threshold for at least a user defined duration.','team analytics',DEFAULT,DEFAULT);

/* drls */
INSERT INTO emsdev_emsrules.document_log(document_log_id,document_uid,document_type,document_raw,document_timestamp,document_filename,external_ref_id,entry_timestamp) VALUES (DEFAULT,(SELECT emsdev_emsrules.rule.document_uid FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'DRL',convert_to('package com.ge.current.em.rules; import java.lang.Double; import java.lang.Math; import java.io.Serializable; import java.util.Arrays; import java.util.ArrayList import java.util.List; import java.util.Map; import com.ge.current.em.entities.analytics.*; import com.ge.current.em.util.TagConstants; import com.ge.current.em.util.GeneralUtil; import com.ge.current.em.util.RulesUtil import com.ge.current.em.util.TagConstants import java.util.Date import java.util.HashMap; import org.slf4j.Logger; import org.slf4j.LoggerFactory; rule "Supply Temperature Too High" dialect "java" no-loop true when $data : RulesBaseFact($assetId: assetId, $tagsMap: tagsMap, $segmentId: segmentId, $enterpriseId: enterpriseId, $siteId: siteId, $measuresAvgMap: measuresAvgMap, $alarmObject: alarmObject, $conditionMet: conditionMet, $parameters: parameters, detectFaultForSupplyTemperatureTooHigh($measuresAvgMap, $parameters, $alarmObject)) then $alarmObject.setAssetId($assetId); $alarmObject.setAlertName(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); $alarmObject.setAlertType(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); $alarmObject.setCategory(RulesUtil.CATEGORY); $alarmObject.setFaultCategory(RulesUtil.FAULT_CATEGORY_PERFORMANCE); $data.setAlarmObject($alarmObject); $data.setConditionMet(true); end function boolean detectFaultForSupplyTemperatureTooHigh(Object measuresObject, Object parametersObject, Object alarmObject) { final Logger LOG = LoggerFactory.getLogger(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); boolean faultDetected = false; if(measuresObject == null || parametersObject == null || alarmObject == null) { return faultDetected; } AlarmObject alarm = (AlarmObject) alarmObject; Map<String, Object> parameters = (Map<String, Object>) parametersObject; List<Map<String, Object>> measures = (List<Map<String, Object>>) measuresObject; String temperatureThresholdKeyName = TagConstants.Parameters.TEMPERATURE_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); String tempHoldDurationKeyName = TagConstants.Parameters.TEMP_HOLD_DURATION.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); String severityKeyName = TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); if(!areTagsValidForSupplyTemperatureTooHigh((Object)measures.get(0), parametersObject)) { return faultDetected; } Double temperatureThreshold = (Double) parameters.get(temperatureThresholdKeyName); Double tempHoldDuration = (Double) parameters.get(tempHoldDurationKeyName); /* detect fault */ Map<String, Object> qualifyingFields = (Map<String, Object>) getConditionMetObjectForSupplyTemperatureTooHigh(measuresObject, temperatureThreshold, tempHoldDuration); faultDetected = (boolean) qualifyingFields.get(RulesBaseFact.CONDITION_MET_FIELDNAME); if(faultDetected) { Double actualDuration = (Double) qualifyingFields.get(TagConstants.Parameters.DURATION.getParameterName()); alarm.setSeverity((Double) parameters.get(severityKeyName)); alarm.setDuration(actualDuration); alarm.setTimeOfAlert(qualifyingFields.get(AlarmObject.START_ALERT_FIELDNAME).toString()); alarm.setFrequency(AlarmObject.FREQUENCY_HOURLY); alarm.setQueryTable(AlarmObject.QUERY_TABLE_BY_MINUTE); alarmObject = alarm; LOG.info("Fault detected! Triggering alarm..."); } return faultDetected; } function boolean areTagsValidForSupplyTemperatureTooHigh(Object firstMapOfMeasures, Object parametersObject) { final Logger LOG = LoggerFactory.getLogger(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); Map<String, Object> measures = (Map<String, Object>) firstMapOfMeasures; Map<String, Object> parameters = (Map<String, Object>) parametersObject; String temperatureThresholdKeyName = TagConstants.Parameters.TEMPERATURE_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); String tempHoldDurationKeyName = TagConstants.Parameters.TEMP_HOLD_DURATION.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); String severityKeyName = TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); List<String> requiredTags = Arrays.asList(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), temperatureThresholdKeyName, tempHoldDurationKeyName, severityKeyName); if(!measures.isEmpty() && !parameters.isEmpty()) { List<String> tagsToBeChecked = new ArrayList<>(); tagsToBeChecked.addAll(measures.keySet()); tagsToBeChecked.addAll(parameters.keySet()); if(!RulesUtil.areTagsValid(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH, tagsToBeChecked, requiredTags)) { LOG.info("Tags are not valid."); return false; } } else { LOG.warn("Measures and parameters are empty."); return false; } return true; } function Object getConditionMetObjectForSupplyTemperatureTooHigh(Object measuresObject, Double temperatureThreshold, Double tempHoldDuration) { final Logger LOG = LoggerFactory.getLogger(RulesUtil.SUPPLY_TEMPERATURE_TOO_HIGH); Double duration = 0.0; String startAlert = null; Map<String, Object> result = new HashMap<>(); result.put(RulesBaseFact.CONDITION_MET_FIELDNAME, false); List<Map<String, Object>> measures = (List<Map<String, Object>>) measuresObject; for(Map<String, Object> currentMeasure : measures) { if(currentMeasure.get(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName()) != null && currentMeasure.get(TagConstants.Measures.EVENT_TS.getMeasureName()) != null) { Double currentDischargeAirTempSensor = (Double) currentMeasure.get(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName()); LOG.info("Checking if current dischargeAirTempSensor: " + currentDischargeAirTempSensor + " is > temperatureThreshold: " + temperatureThreshold); if(currentDischargeAirTempSensor > temperatureThreshold) { duration += 15.0; LOG.info("Checking if current duration: " + duration + " is >= tempHoldDuration: " + tempHoldDuration); if(duration >= tempHoldDuration) { if(result.get(AlarmObject.START_ALERT_FIELDNAME) == null) { startAlert = currentMeasure.get(TagConstants.Measures.EVENT_TS.getMeasureName()).toString(); result.put(AlarmObject.START_ALERT_FIELDNAME, startAlert); } result.put(RulesBaseFact.CONDITION_MET_FIELDNAME, true); result.put(TagConstants.Parameters.DURATION.getParameterName(), duration); } } } } LOG.info("Result: " + result); return (Object) result; } ', 'UTF8'),localtimestamp,'SupplyTemperatureTooHigh.drl',NULL,DEFAULT);
INSERT INTO emsdev_emsrules.document_log(document_log_id,document_uid,document_type,document_raw,document_timestamp,document_filename,external_ref_id,entry_timestamp) VALUES (DEFAULT,(SELECT emsdev_emsrules.rule.document_uid FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'DRL',convert_to('package com.ge.current.em.rules; import java.lang.Double; import java.lang.Math; import java.io.Serializable; import java.util.Arrays; import java.util.ArrayList import java.util.List; import java.util.Map; import com.ge.current.em.entities.analytics.*; import com.ge.current.em.util.TagConstants; import com.ge.current.em.util.GeneralUtil; import com.ge.current.em.util.RulesUtil import com.ge.current.em.util.TagConstants import java.util.Date import java.util.HashMap; import org.slf4j.Logger; import org.slf4j.LoggerFactory; rule "Supply Temperature Too Low" dialect "java" no-loop true when $data : RulesBaseFact($assetId: assetId, $tagsMap: tagsMap, $segmentId: segmentId, $enterpriseId: enterpriseId, $siteId: siteId, $measuresAvgMap: measuresAvgMap, $alarmObject: alarmObject, $conditionMet: conditionMet, $parameters: parameters, detectFaultForSupplyTemperatureTooLow($measuresAvgMap, $parameters, $alarmObject)) then $alarmObject.setAssetId($assetId); $alarmObject.setAlertName(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); $alarmObject.setAlertType(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); $alarmObject.setCategory(RulesUtil.CATEGORY); $alarmObject.setFaultCategory(RulesUtil.FAULT_CATEGORY_PERFORMANCE); $data.setAlarmObject($alarmObject); $data.setConditionMet(true); end function boolean detectFaultForSupplyTemperatureTooLow(Object measuresObject, Object parametersObject, Object alarmObject) { final Logger LOG = LoggerFactory.getLogger(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); boolean faultDetected = false; if(measuresObject == null || parametersObject == null || alarmObject == null) { return faultDetected; } AlarmObject alarm = (AlarmObject) alarmObject; List<Map<String, Object>> measures = (List<Map<String, Object>>) measuresObject; Map<String, Object> parameters = (Map<String, Object>) parametersObject; String temperatureThresholdKeyName = TagConstants.Parameters.TEMPERATURE_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); String tempHoldDurationKeyName = TagConstants.Parameters.TEMP_HOLD_DURATION.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); String severityKeyName = TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); if(!areTagsValidForSupplyTemperatureTooLow((Object)measures.get(0), parametersObject)) { return faultDetected; } Double temperatureThreshold = (Double) parameters.get(temperatureThresholdKeyName); Double tempHoldDuration = (Double) parameters.get(tempHoldDurationKeyName); /* detect fault */ Map<String, Object> qualifyingFields = (Map<String, Object>) getConditionMetObjectForSupplyTemperatureTooLow(measuresObject, temperatureThreshold, tempHoldDuration); faultDetected = (boolean) qualifyingFields.get(RulesBaseFact.CONDITION_MET_FIELDNAME); if(faultDetected) { Double actualDuration = (Double) qualifyingFields.get(TagConstants.Parameters.DURATION.getParameterName()); alarm.setSeverity((Double) parameters.get(severityKeyName)); alarm.setDuration(actualDuration); alarm.setTimeOfAlert(qualifyingFields.get(AlarmObject.START_ALERT_FIELDNAME).toString()); alarm.setFrequency(AlarmObject.FREQUENCY_HOURLY); alarm.setQueryTable(AlarmObject.QUERY_TABLE_BY_MINUTE); alarmObject = alarm; LOG.info("Fault detected! Triggering alarm..."); } return faultDetected; } function boolean areTagsValidForSupplyTemperatureTooLow(Object firstMapOfMeasures, Object parametersObject) { final Logger LOG = LoggerFactory.getLogger(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); String temperatureThresholdKeyName = TagConstants.Parameters.TEMPERATURE_THRESHOLD.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); String tempHoldDurationKeyName = TagConstants.Parameters.TEMP_HOLD_DURATION.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); String severityKeyName = TagConstants.Parameters.SEVERITY.getParameterName() + RulesUtil.getFDSINumber(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); Map<String, Object> measures = (Map<String, Object>) firstMapOfMeasures; Map<String, Object> parameters = (Map<String, Object>) parametersObject; List<String> requiredTags = Arrays.asList(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName(), temperatureThresholdKeyName, tempHoldDurationKeyName, severityKeyName); if(!measures.isEmpty() && !parameters.isEmpty()) { List<String> tagsToBeChecked = new ArrayList<>(); tagsToBeChecked.addAll(measures.keySet()); tagsToBeChecked.addAll(parameters.keySet()); if(!RulesUtil.areTagsValid(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW, tagsToBeChecked, requiredTags)) { LOG.info("Tags are not valid."); return false; } } else { LOG.warn("Measures and parameters are empty."); return false; } return true; } function Object getConditionMetObjectForSupplyTemperatureTooLow(Object measuresObject, Double temperatureThreshold, Double tempHoldDuration) { final Logger LOG = LoggerFactory.getLogger(RulesUtil.SUPPLY_TEMPERATURE_TOO_LOW); Double duration = 0.0; String startAlert = null; Map<String, Object> result = new HashMap<>(); result.put(RulesBaseFact.CONDITION_MET_FIELDNAME, false); List<Map<String, Object>> measures = (List<Map<String, Object>>) measuresObject; for(Map<String, Object> currentMeasure : measures) { if(currentMeasure.get(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName()) != null && currentMeasure.get(TagConstants.Measures.EVENT_TS.getMeasureName()) != null) { Double currentDischargeAirTempSensor = (Double) currentMeasure.get(TagConstants.Tags.DISCHARGE_AIR_TEMP_SENSOR.getTagName()); LOG.info("Checking if current dischargeAirTempSensor: " + currentDischargeAirTempSensor + " is < temperatureThreshold: " + temperatureThreshold); if(currentDischargeAirTempSensor < temperatureThreshold) { duration += 15.0; LOG.info("Checking if current duration: " + duration + " is >= tempHoldDuration: " + tempHoldDuration); if(duration >= tempHoldDuration) { if(result.get(AlarmObject.START_ALERT_FIELDNAME) == null) { startAlert = currentMeasure.get(TagConstants.Measures.EVENT_TS.getMeasureName()).toString(); result.put(AlarmObject.START_ALERT_FIELDNAME, startAlert); } result.put(RulesBaseFact.CONDITION_MET_FIELDNAME, true); result.put(TagConstants.Parameters.DURATION.getParameterName(), duration); } } } } LOG.info("Result: " + result); return (Object) result; } ', 'UTF8'),localtimestamp,'SupplyTemperatureTooLow.drl',NULL,DEFAULT);

/* measures */
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-req-measures', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','REQUIRED_MEASURES_AVG','','dischargeAirTempSensor','','String','N/A',DEFAULT,'REQUIRED_MEASURES',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-req-measures', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','REQUIRED_MEASURES_AVG','','dischargeAirTempSensor','','String','N/A',DEFAULT,'REQUIRED_MEASURES',DEFAULT,DEFAULT);

/* asset types */
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-asset-type-a', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-asset-type-b', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-asset-type-c', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-asset-type-d', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-asset-type-e', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-asset-type-a', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-asset-type-b', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-asset-type-c', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-asset-type-d', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-asset-type-e', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);


/* configurable parameters */
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-a', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','temperatureThreshold','Temperature Threshold','74','74','DOUBLE','F',DEFAULT,'Temperature Threshold',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-b', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'MIN-IN-PARAMETER','temperatureThreshold.min','Minimum Temperature Threshold','60','60','DOUBLE','F',0,'Minimum Temperature Threshold',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-c', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'MAX-IN-PARAMETER','temperatureThreshold.max','Maximum Temperature Threshold','80','80','DOUBLE','F',0,'Maximum Temperature Threshold',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-d', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','duration','Duration','60','60','DOUBLE','minutes',DEFAULT,'Duration',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-e', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'MIN-IN-PARAMETER','duration.min','Minimum Duration','15','15','DOUBLE','minutes',0,'Minimum Duration',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-f', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'MAX-IN-PARAMETER','duration.max','Maximum Duration','240','240','DOUBLE','minutes',0,'Maximum Duration',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-g', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'IN-PARAMETER','severity','Severity','3','3','DOUBLE','numeric',DEFAULT,'Rule Severity',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-h', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'MIN-IN-PARAMETER','severity.min','Minimum Severity','1','1','DOUBLE','numeric',0,'Minimum Rule Severity',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-24-i', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-24'),'MAX-IN-PARAMETER','severity.max','Maximum Severity','4','4','DOUBLE','numeric',0,'Maximum Rule Severity',DEFAULT,DEFAULT);

INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-a', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','temperatureThreshold','Temperature Threshold','66','66','DOUBLE','F',DEFAULT,'Temperature Threshold',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-b', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'MIN-IN-PARAMETER','temperatureThreshold.min','Minimum Temperature Threshold','60','60','DOUBLE','F',0,'Minimum Temperature Threshold',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-c', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'MAX-IN-PARAMETER','temperatureThreshold.max','Maximum Temperature Threshold','80','80','DOUBLE','F',0,'Maximum Temperature Threshold',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-d', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','duration','Duration','60','60','DOUBLE','minutes',DEFAULT,'Duration',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-e', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'MIN-IN-PARAMETER','duration.min','Minimum Duration','15','15','DOUBLE','minutes',0,'Minimum Duration',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-f', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'MAX-IN-PARAMETER','duration.max','Maximum Duration','240','240','DOUBLE','minutes',0,'Maximum Duration',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-g', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'IN-PARAMETER','severity','Severity','3','3','DOUBLE','numeric',DEFAULT,'Rule Severity',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-h', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'MIN-IN-PARAMETER','severity.min','Minimum Severity','1','1','DOUBLE','numeric',0,'Minimum Rule Severity',DEFAULT,DEFAULT);
INSERT INTO emsdev_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-25-i', (SELECT emsdev_emsrules.rule.rule_id FROM emsdev_emsrules.rule WHERE emsdev_emsrules.rule.rule_uid='RULE-UID-25'),'MAX-IN-PARAMETER','severity.max','Maximum Severity','4','4','DOUBLE','numeric',0,'Maximum Rule Severity',DEFAULT,DEFAULT);
