package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class RulesBaseFact implements Serializable{

	private String assetId;
	private String ruleType;
	private Map<String, Double> measures;
	private Map<String,String> tags;
	private String segmentId;
	private String enterpriseId;
	private String siteId;
	private List<AHUBean> ahuList;
	
	public String getAssetId() {
		return assetId;
	}
	public void setAssetId(String assetId) {
		this.assetId = assetId;
	}
	public String getRuleType() {
		return ruleType;
	}
	public void setRuleType(String ruleType) {
		this.ruleType = ruleType;
	}
	public Map<String, Double> getMeasures() {
		return measures;
	}
	public void setMeasures(Map<String, Double> measures) {
		this.measures = measures;
	}
	public Map<String, String> getTags() {
		return tags;
	}
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
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
	public List<AHUBean> getAhuList() {
		return ahuList;
	}
	public void setAhuList(List<AHUBean> ahuList) {
		this.ahuList = ahuList;
	}
	
}
