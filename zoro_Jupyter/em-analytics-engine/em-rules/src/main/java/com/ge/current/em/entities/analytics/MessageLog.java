package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Direct copy from spark-engine repo, this file must be
 * moved to spark-engine
 */
public class MessageLog implements Serializable {

    public static final Map<String, String> PAIRS = new HashMap<>();
    public static final Map<String, String> REVERSE_PAIRS = new HashMap<>();

    private String timeBucket;
    private String processUid;
    private Map<String, String> tags;
    private UUID logUUID;
    private Date errorTs;
    private String appKey;
    private String category;
    private String destURI;
    private String jobUID;
    private String msg;
    private String msgExternalURI;
    private String msgUID;
    private String originURI;
    private String status;
    private String msgExtUri;
    private String msgSumry;
    private String solrQuery;

    public MessageLog() {
    }

    public MessageLog(String timeBucket, String processUid, Map<String, String> tags, UUID logUUID, Date errorTs,
            String appKey, String category, String destURI, String jobUID, String msg, String msgExternalURI,
            String msgUID, String originURI, String status, String msgExtUri, String msgSumry) {
        this.timeBucket = timeBucket;
        this.processUid = processUid;
        this.tags = tags;
        this.logUUID = logUUID;
        this.errorTs = errorTs;
        this.appKey = appKey;
        this.category = category;
        this.destURI = destURI;
        this.jobUID = jobUID;
        this.msg = msg;
        this.msgExternalURI = msgExternalURI;
        this.msgUID = msgUID;
        this.originURI = originURI;
        this.status = status;
        this.msgExtUri = msgExtUri;
        this.msgSumry = msgSumry;
    }

    public String getTimeBucket() {
        return timeBucket;
    }

    public void setTimeBucket(String timeBucket) {
        this.timeBucket = timeBucket;
    }

    public String getProcessUid() {
        return processUid;
    }

    public void setProcessUid(String processUid) {
        this.processUid = processUid;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public UUID getLogUUID() {
        return logUUID;
    }

    public void setLogUUID(UUID logUUID) {
        this.logUUID = logUUID;
    }

    public Date getErrorTs() {
        return errorTs;
    }

    public void setErrorTs(Date errorTs) {
        this.errorTs = errorTs;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDestURI() {
        return destURI;
    }

    public void setDestURI(String destURI) {
        this.destURI = destURI;
    }

    public String getJobUID() {
        return jobUID;
    }

    public void setJobUID(String jobUID) {
        this.jobUID = jobUID;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getMsgExternalURI() {
        return msgExternalURI;
    }

    public void setMsgExternalURI(String msgExternalURI) {
        this.msgExternalURI = msgExternalURI;
    }

    public String getMsgUID() {
        return msgUID;
    }

    public void setMsgUID(String msgUID) {
        this.msgUID = msgUID;
    }

    public String getOriginURI() {
        return originURI;
    }

    public void setOriginURI(String originURI) {
        this.originURI = originURI;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMsgExtUri() {
        return msgExtUri;
    }

    public void setMsgExtUri(String msgExtUri) {
        this.msgExtUri = msgExtUri;
    }

    public String getMsgSumry() {
        return msgSumry;
    }

    public void setMsgSumry(String msgSumry) {
        this.msgSumry = msgSumry;
    }

    public String getSolrQuery() {
        return solrQuery;
    }

    public void setSolrQuery(String solrQuery) {
        this.solrQuery = solrQuery;
    }

    @Override public String toString() {
        return "MessageLog{" +
                "timeBucket='" + timeBucket + '\'' +
                ", processUid='" + processUid + '\'' +
                ", tags=" + tags +
                ", logUUID=" + logUUID +
                ", errorTs=" + errorTs +
                ", appKey='" + appKey + '\'' +
                ", category='" + category + '\'' +
                ", destURI='" + destURI + '\'' +
                ", jobUID='" + jobUID + '\'' +
                ", msg='" + msg + '\'' +
                ", msgExternalURI='" + msgExternalURI + '\'' +
                ", msgUID='" + msgUID + '\'' +
                ", originURI='" + originURI + '\'' +
                ", status='" + status + '\'' +
                ", msgExtUri='" + msgExtUri + '\'' +
                ", msgSumry='" + msgSumry + '\'' +
                ", solrQuery='" + solrQuery + '\'' +
                '}';
    }

    static {
        PAIRS.put("timeBucket", "time_bucket");
        PAIRS.put("processUid", "process_uid");
        PAIRS.put("errorTs", "error_ts");
        PAIRS.put("logUUID", "log_uuid");
        PAIRS.put("appKey", "app_key");
        PAIRS.put("category", "category");
        PAIRS.put("destURI", "dest_uri");
        PAIRS.put("jobUID", "job_uid");
        PAIRS.put("msg", "msg");
        PAIRS.put("msgExternalURI", "msg_ext_uri");
        PAIRS.put("msgSumry", "msg_sumry");
        PAIRS.put("msgUID", "msg_uid");
        PAIRS.put("originURI", "origin_uri");
        PAIRS.put("solrQuery", "solr_query");
        PAIRS.put("status", "status");
        PAIRS.put("tags", "tags");

        PAIRS.entrySet().forEach(e -> REVERSE_PAIRS.put(e.getValue(), e.getKey()));
    }
}
