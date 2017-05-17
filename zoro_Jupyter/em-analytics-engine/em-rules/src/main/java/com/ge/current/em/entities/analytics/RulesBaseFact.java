package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class RulesBaseFact implements Serializable{

	public static final String CONDITION_MET_FIELDNAME = "conditionMet";

	private String assetId;
	private String assetType;
	
	private List<Map<String, Object>> measuresAvgMap;
	private List<Map<String, Object>> measuresMap;
	private List<Map<String, Object>> measuresAggrMap;

	private List<Map<String, Object>> tagsMap;
	private String segmentId;
	private String enterpriseId;
	private String YYYYMMdd;
	private String hhmm;
	private String siteId;
	
	private AlarmObject alarmObject;
	private Boolean conditionMet;
	private Map<String, Object> parameters;
	private Map<String, String> ext_properties;

	public RulesBaseFact() {
		
	}

	public RulesBaseFact(String assetId, String assetType, List<Map<String, Object>> tagsMap, String segmentId,
			String enterpriseId, String hhmm, String siteId, List<Map<String, Object>> measuresAvgMap,
			AlarmObject alarmObject, Boolean conditionMet, Map<String, Object> parameters) {
		this.assetId = assetId;
		this.assetType = assetType;
		this.tagsMap = tagsMap;
		this.segmentId = segmentId;
		this.enterpriseId = enterpriseId;
		this.hhmm = hhmm;
		this.siteId = siteId;
		this.measuresAvgMap = measuresAvgMap;
		this.alarmObject = alarmObject;
		this.conditionMet = conditionMet;
		this.parameters = parameters;
	}

	public String getAssetId() {
		return assetId;
	}
	public void setAssetId(String assetId) {
		this.assetId = assetId;
	}

	public String getSegmentId() {
		return segmentId;
	}
	public void setSegmentId(String segmentId) {
		this.segmentId = segmentId;
	}
	public String getEnterpriseId() {
		return enterpriseId;
	}
	public void setEnterpriseId(String enterpriseId) {
		this.enterpriseId = enterpriseId;
	}
	public String getSiteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, Object> parameters) {
		this.parameters = parameters;
	}

	public List<Map<String, Object>> getTagsMap() {
		return tagsMap;
	}
	public void setTagsMap(List<Map<String, Object>> tagsMap) {
		this.tagsMap = tagsMap;
	}
	public AlarmObject getAlarmObject() {
		return alarmObject;
	}
	public void setAlarmObject(AlarmObject alarmObject) {
		this.alarmObject = alarmObject;
	}
	public Boolean getConditionMet() {
		return conditionMet;
	}
	public void setConditionMet(Boolean conditionMet) {
		this.conditionMet = conditionMet;
	}

	public String getHhmm() {
		return hhmm;
	}

	public void setHhmm(String hhmm) {
		this.hhmm = hhmm;
	}

	public String getAssetType() {
		return assetType;
	}

	public void setAssetType(String assetType) {
		this.assetType = assetType;
	}

	public Map<String, String> getExt_properties() {
		return ext_properties;
	}

	public void setExt_properties(Map<String, String> ext_properties) {
		this.ext_properties = ext_properties;
	}

	public String getYYYYMMdd() {
		return YYYYMMdd;
	}

	public void setYYYYMMdd(String yYYYMMdd) {
		YYYYMMdd = yYYYMMdd;
	}

	public List<Map<String, Object>> getMeasuresAvgMap() {
		return measuresAvgMap;
	}

	public List<Map<String, Object>> getMeasuresAggrMap() {
		return measuresAggrMap;
	}

	public void setMeasuresAvgMap(List<Map<String, Object>> measuresAvgMap) {
		this.measuresAvgMap = measuresAvgMap;
	}

	public void setMeasuresAggrMap(List<Map<String, Object>> measuresAggrMap) {
		this.measuresAggrMap = measuresAggrMap;
	}


	public List<Map<String, Object>> getMeasuresMap() {
		return measuresMap;
	}

	public void setMeasuresMap(List<Map<String, Object>> measuresMap) {
		this.measuresMap = measuresMap;
	}


	@Override public String toString() {
		return "RulesBaseFact{" + "assetId='" + assetId + '\'' + ", assetType='" + assetType + '\'' + ", tagsMap="
				+ tagsMap + ", segmentId='" + segmentId + '\'' + ", enterpriseId='" + enterpriseId + '\'' + ", hhmm='"
				+ hhmm + '\'' + ", siteId='" + siteId + '\'' + ", measuresAvgMap=" + measuresAvgMap + ", measuresAggrMap=" + measuresAggrMap + ", alarmObject="
				+ alarmObject + ", conditionMet=" + conditionMet + ", parameters=" + parameters + '}';
	}
}
