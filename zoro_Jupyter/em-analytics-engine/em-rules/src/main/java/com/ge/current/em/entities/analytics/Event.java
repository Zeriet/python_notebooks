package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.Map;

public class Event implements Serializable {

	public static final String EXT_PROP_SITE_UID = "site_uid";

	private String assetId;
	private String assetType;
	private String enterpriseId;
	private String segmentId;
	private String siteId;
	private Map<String, Object> measuresAvg;
	private Map<String, Object> measuresAggr;
	private Map<String, Object> measures;
	private Map<String, String> tags;
	private Map<String, String> extProps;
	private String yymmdd;
	private String hhmm;
	private String hh;
	private String mm;

	public String getAssetId() {
		return assetId;
	}
	public void setAssetId(String assetId) {
		this.assetId = assetId;
	}
	public String getEnterpriseId() {
		return enterpriseId;
	}
	public void setEnterpriseId(String enterpriseId) {
		this.enterpriseId = enterpriseId;
	}
	public String getSegmentId() {
		return segmentId;
	}
	public void setSegmentId(String segmentId) {
		this.segmentId = segmentId;
	}
	public String getSiteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}
	
	public Map<String, String> getTags() {
		return tags;
	}
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	public Map<String, String> getExtProps() {
		return extProps;
	}
	public void setExtProps(Map<String, String> extProps) {
		this.extProps = extProps;
	}
	public String getYymmdd() {
		return yymmdd;
	}
	public void setYymmdd(String yymmdd) {
		this.yymmdd = yymmdd;
	}
	public String getHhmm() {
		return hhmm;
	}
	public void setHhmm(String hhmm) {
		this.hhmm = hhmm;
	}
	public String getHh() {
		return hh;
	}
	public void setHh(String hh) {
		this.hh = hh;
	}
	public String getMm() {
		return mm;
	}
	public void setMm(String mm) {
		this.mm = mm;
	}

	public Map<String, Object> getMeasures() {
		return measures;
	}

	public void setMeasures(Map<String, Object> measures) {
		this.measures = measures;
	}

	private String getMM(String hhmm) {
		char[] mm = new char[] {hhmm.charAt(2), hhmm.charAt(3)};
		return new String(mm);
	}

	private String gethh(String hhmm) {
		char[] mm = new char[] {hhmm.charAt(0), hhmm.charAt(1)};
		return new String(mm);
	}

	public String getAssetType() {
		return assetType;
	}

	public void setAssetType(String assetType) {
		this.assetType = assetType;
	}
	public Map<String, Object> getMeasuresAvg() {
		return measuresAvg;
	}
	public Map<String, Object> getMeasuresAggr() {
		return measuresAggr;
	}
	public void setMeasuresAvg(Map<String, Object> measuresAvg) {
		this.measuresAvg = measuresAvg;
	}
	public void setMeasuresAggr(Map<String, Object> measuresAggr) {
		this.measuresAggr = measuresAggr;
	}
	
}
