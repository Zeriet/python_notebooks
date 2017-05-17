package com.ge.current.em.aggregation.dao;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NormalizedEventLog implements Serializable{
	
	private String timeBucket;
	private String resourceUid;
	private String eventType;
	private Date eventTimestamp;
	private UUID logUUID;
	private Date event_ts_loc;
	private String event_ts_tz;
    private String enterpriseUid;
	private String resrc_type;
	private String regionName;
	private String siteCity;
	private String siteState;
	private String siteCountry;
	private String siteUID;
	private String siteName;
	private String zoneUid;
	private String zoneName;
	private String segmentUid;
	private String segmentName;
	private String assetUid;
	private String assetType;
	private String pointUid;
	private String pointType;
	private String solr_query;
	List<String> labels;
	Map<String, String> tags;
	Map<String, Double> measures;


	public NormalizedEventLog() {
	}

	public NormalizedEventLog(String timeBucket, String enterpriseUid, String resourceUid, String eventType, Date eventTimestamp, UUID logUUID, Date event_ts_loc, String event_ts_tz, String resrc_type, String regionName, String siteCity, String siteState, String siteCountry, String siteUID, String siteName, String zoneUid, String zoneName, String segmentUid, String segmentName, String assetUid, String assetType, String pointUid, String pointType, List<String> labels, Map<String, String> tags, Map<String, Double> measures) {
		this.timeBucket = timeBucket;
		this.enterpriseUid = enterpriseUid;
		this.resourceUid = resourceUid;
		this.eventType = eventType;
		this.eventTimestamp = eventTimestamp;
		this.logUUID = logUUID;
		this.event_ts_loc = event_ts_loc;
		this.event_ts_tz = event_ts_tz;
		this.resrc_type = resrc_type;
		this.regionName = regionName;
		this.siteCity = siteCity;
		this.siteState = siteState;
		this.siteCountry = siteCountry;
		this.siteUID = siteUID;
		this.siteName = siteName;
		this.zoneUid = zoneUid;
		this.zoneName = zoneName;
		this.segmentUid = segmentUid;
		this.segmentName = segmentName;
		this.assetUid = assetUid;
		this.assetType = assetType;
		this.pointUid = pointUid;
		this.pointType = pointType;
		this.labels = labels;
		this.tags = tags;
		this.measures = measures;
	}

	public String getTimeBucket() {
		return timeBucket;
	}

	public void setTimeBucket(String timeBucket) {
		this.timeBucket = timeBucket;
	}

	public String getEnterpriseUid() {
		return enterpriseUid;
	}

	public void setEnterpriseUid(String enterpriseUid) {
		this.enterpriseUid = enterpriseUid;
	}

	public String getResrc_Uid() {
		return resourceUid;
	}

	public void setResourceUid(String resourceUid) {
		this.resourceUid = resourceUid;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public Date getEvent_Ts() {
		return eventTimestamp;
	}

	public void setEventTimestamp(Date eventTimestamp) {
		this.eventTimestamp = eventTimestamp;
	}

	public UUID getLogUUID() {
		return logUUID;
	}

	public void setLogUUID(UUID logUUID) {
		this.logUUID = logUUID;
	}

	public Date getEvent_ts_loc() {
		return event_ts_loc;
	}

	public void setEvent_ts_loc(Date event_ts_loc) {
		this.event_ts_loc = event_ts_loc;
	}

	public String getEvent_ts_tz() {
		return event_ts_tz;
	}

	public void setEvent_ts_tz(String event_ts_tz) {
		this.event_ts_tz = event_ts_tz;
	}

	public String getResrc_type() {
		return resrc_type;
	}

	public void setResrc_type(String resrc_type) {
		this.resrc_type = resrc_type;
	}

	public String getRegionName() {
		return regionName;
	}

	public void setRegionName(String regionName) {
		this.regionName = regionName;
	}

	public String getSiteCity() {
		return siteCity;
	}

	public void setSiteCity(String siteCity) {
		this.siteCity = siteCity;
	}

	public String getSiteState() {
		return siteState;
	}

	public void setSiteState(String siteState) {
		this.siteState = siteState;
	}

	public String getSiteCountry() {
		return siteCountry;
	}

	public void setSiteCountry(String siteCountry) {
		this.siteCountry = siteCountry;
	}

	public String getSiteUID() {
		return siteUID;
	}

	public void setSiteUID(String siteUID) {
		this.siteUID = siteUID;
	}

	public String getSiteName() {
		return siteName;
	}

	public void setSiteName(String siteName) {
		this.siteName = siteName;
	}

	public String getZoneUid() {
		return zoneUid;
	}

	public void setZoneUid(String zoneUid) {
		this.zoneUid = zoneUid;
	}

	public String getZoneName() {
		return zoneName;
	}

	public void setZoneName(String zoneName) {
		this.zoneName = zoneName;
	}

	public String getSegmentUid() {
		return segmentUid;
	}

	public void setSegmentUid(String segmentUid) {
		this.segmentUid = segmentUid;
	}

	public String getSegmentName() {
		return segmentName;
	}

	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}

	public String getAssetUid() {
		return assetUid;
	}

	public void setAssetUid(String assetUid) {
		this.assetUid = assetUid;
	}

	public String getAssetType() {
		return assetType;
	}

	public void setAssetType(String assetType) {
		this.assetType = assetType;
	}

	public String getPointUid() {
		return pointUid;
	}

	public void setPointUid(String pointUid) {
		this.pointUid = pointUid;
	}

	public String getPointType() {
		return pointType;
	}

	public void setPointType(String pointType) {
		this.pointType = pointType;
	}

	public String getSolr_query() { return solr_query; }

	public void setSolr_query(String solr_query) { this.solr_query = solr_query; }

	public List<String> getLabels() {
		return labels;
	}

	public void setLabels(List<String> labels) {
		this.labels = labels;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public Map<String, Double> getMeasures() {
		return measures;
	}

	public void setMeasures(Map<String, Double> measures) {
		this.measures = measures;
	}

	@Override
	public String toString() {
		return "EventLog{" +
				"timeBucket='" + timeBucket + '\'' +
				", enterpriseUid='" + enterpriseUid + '\'' +
				", resourceUid='" + resourceUid + '\'' +
				", eventType='" + eventType + '\'' +
				", eventTimestamp=" + eventTimestamp +
				", logUUID=" + logUUID +
				", event_ts_loc=" + event_ts_loc +
				", event_ts_tz='" + event_ts_tz + '\'' +
				", resrc_type='" + resrc_type + '\'' +
				", regionName='" + regionName + '\'' +
				", siteCity='" + siteCity + '\'' +
				", siteState='" + siteState + '\'' +
				", siteCountry='" + siteCountry + '\'' +
				", siteUID='" + siteUID + '\'' +
				", siteName='" + siteName + '\'' +
				", zoneUid='" + zoneUid + '\'' +
				", zoneName='" + zoneName + '\'' +
				", segmentUid='" + segmentUid + '\'' +
				", segmentName='" + segmentName + '\'' +
				", assetUid='" + assetUid + '\'' +
				", assetType='" + assetType + '\'' +
				", pointUid='" + pointUid + '\'' +
				", pointType='" + pointType + '\'' +
				", labels=" + labels +
				", tags=" + tags +
				", measures=" + measures +
				'}';
	}
}