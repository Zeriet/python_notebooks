package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AlarmObject implements Serializable {

    public static final String QUERY_TABLE_BY_MINUTE = "by_minute";
    public static final String QUERY_TABLE_BY_NORM_LOG = "norm_log";

    public static final String FREQUENCY_HOURLY = "hourly";
    public static final String FREQUENCY_DAILY = "daily";
    public static final String FREQUENCY_MONTHLY = "monthly";
    public static final String FREQUENCY_YEARLY = "yearly";
    public static final String FREQUENCY_15MINUTE = "15_minute";
    public static final String FREQUENCY_5MINUTE = "5_minute";

    public static final String START_ALERT_FIELDNAME = "startAlert";


    private String assetId;
    private Double severity;
    private Double duration;
    private String enterpriseId;
    private String siteId;
    private Map<String, List<Object>> segments;
    private Integer incrementMins;
    private String assetType;
    private String alertName;
    private String alertType;
    private String assetName;
    private String timeOfAlert;
    private String category;
    private String faultCategory;
    private String resrcUid;
    private String resrcType;
    private String frequency;
    private String queryTable;
    private Map<String, String> extProps;

    public AlarmObject() {

    }

    public AlarmObject(String assetId, Double severity, Double duration, String enterpriseId, String siteId,
            Map<String, List<Object>> segments, Integer incrementMins, String assetType, String alertName,
            String alertType, String assetName, String category, String timeOfAlert, String faultCategory,
            String resrcUid, String resrcType) {
        this.assetId = assetId;
        this.severity = severity;
        this.duration = duration;
        this.enterpriseId = enterpriseId;
        this.siteId = siteId;
        this.segments = segments;
        this.incrementMins = incrementMins;
        this.assetType = assetType;
        this.alertName = alertName;
        this.alertType = alertType;
        this.assetName = assetName;
        this.category = category;
        this.timeOfAlert = timeOfAlert;
        this.faultCategory = faultCategory;
        this.resrcUid = resrcUid;
        this.resrcType = resrcType;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public Double getSeverity() {
        return severity;
    }

    public void setSeverity(Double severity) {
        this.severity = severity;
    }

    public Double getDuration() {
        return duration;
    }

    public void setDuration(Double duration) {
        this.duration = duration;
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

    public Map<String, List<Object>> getSegments() {
        return segments;
    }

    public void setSegments(Map<String, List<Object>> segments) {
        this.segments = segments;
    }

    public Integer getIncrementMins() {
        return incrementMins;
    }

    public void setIncrementMins(Integer incrementMins) {
        this.incrementMins = incrementMins;
    }

    public String getAssetType() {
        return assetType;
    }

    public void setAssetType(String assetType) {
        this.assetType = assetType;
    }

    public String getAlertName() {
        return alertName;
    }

    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    public String getAssetName() {
        return assetName;
    }

    public void setAssetName(String assetName) {
        this.assetName = assetName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getTimeOfAlert() {
        return timeOfAlert;
    }

    public void setTimeOfAlert(String timeOfAlert) {
        this.timeOfAlert = timeOfAlert;
    }

    public String getAlertType() {
        return alertType;
    }

    public String getFaultCategory() {
        return faultCategory;
    }

    public void setFaultCategory(String faultCategory) {
        this.faultCategory = faultCategory;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getResrcUid() {
        return resrcUid;
    }

    public void setResrcUid(String resrcUid) {
        this.resrcUid = resrcUid;
    }

    public String getResrcType() {
        return resrcType;
    }

    public void setResrcType(String resrcType) {
        this.resrcType = resrcType;
    }

    public String getQueryTable() {
        return queryTable;
    }

    public void setQueryTable(String queryTable) {
        this.queryTable = queryTable;
    }

    public Map<String, String> getExtProps() {
        return extProps;
    }

    public void setExtProps(Map<String, String> extProps) {
        this.extProps = extProps;
    }

    public String getFrequency() {
        return frequency;
    }

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "AlarmObject{" + "assetId='" + assetId + '\'' + ", severity=" + severity + ", duration=" + duration
                + ", enterpriseId='" + enterpriseId + '\'' + ", siteId='" + siteId + '\'' + ", segments=" + segments
                + ", incrementMins=" + incrementMins + ", assetType='" + assetType + '\'' + ", alertName='" + alertName
                + '\'' + ", alertType='" + alertType + '\'' + ", assetName='" + assetName + '\'' + ", timeOfAlert='"
                + timeOfAlert + '\'' + ", category='" + category + '\'' + ", faultCategory='" + faultCategory + '\''
                + ", resrcUid='" + resrcUid + '\'' + ", resrcType='" + resrcType + '\'' + ", frequency='" + frequency
                + '\'' + ", queryTable='" + queryTable + '\'' + ", extProps=" + extProps + '}';
    }
}
