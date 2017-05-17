package com.ge.current.em.analytics.dto;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class JaceEvent implements Serializable {
    public String timeBucket;

    public String assetUid;

    public String locUid;

    public String eventType;

    public long eventTs;

    public ZonedDateTime zonedDateTime;

    public Map<String, Double> measures;
    public Map<String, Double> rawMeasures;

    public Map<String, String> tags;
    public Map<String, String> rawTags;

    public JaceEvent() {
        measures = new HashMap<>();
        tags = new HashMap<>();
        rawTags = new HashMap<>();
        rawMeasures = new HashMap();
    }

    public JaceEvent(String assetUid,
                     String eventType,
                     Map<String, Double> measures,
                     Map<String, String> tags,
                     long eventTs,
                     ZonedDateTime zonedDateTime) {

        this.assetUid = assetUid;
        this.measures = measures;
        this.eventTs = eventTs;
        this.zonedDateTime = zonedDateTime;
        this.eventType = eventType;
        this.tags = tags;
    }

    public Map<String, Double> getMeasures() { return measures; }

    public Map<String, Double> getRawMeasures() { return rawMeasures; }

    public Map<String, String> getTags() { return tags; }
    public Map<String, String> getRawTags() { return rawTags; }

    public String getAssetUid() { return assetUid; }

    public String getEventType() { return eventType; }

    public long getEventTs() { return eventTs; }

    public ZonedDateTime getZonedDateTime() { return zonedDateTime; }

    public void setMeasures(Map<String, Double> measures) {
        this.measures = measures;
    }

    public void setEventTs(long eventTs) {
        this.eventTs = eventTs;
    }

    public void setAssetUid(String assetUid) {
        this.assetUid = assetUid;
    }

    public void setEventType(String eventType) { this.eventType = eventType; }

    public void setZonedDateTime(ZonedDateTime zonedDateTime) { this.zonedDateTime = zonedDateTime; }

    public void setTags(Map<String, String> tags) { this.tags = tags; }
    public void setRawTags(Map<String, String> rawTags) { this.rawTags = rawTags; }
    public void setRawMeasures(Map<String, Double> rawMeasures) { this.rawMeasures = rawMeasures; }

    @Override
    public String toString() {
        return "JaceEvent{" +
                "timeBucket='" + timeBucket + '\'' +
                ", assetUid='" + assetUid + '\'' +
                ", locUid='" + locUid + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTs=" + eventTs +
                ", zonedDateTime=" + zonedDateTime +
                ", measures=" + measures +
                ", rawMeasures=" + rawMeasures +
                ", tags=" + tags +
                ", rawTags=" + rawTags +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JaceEvent jaceEvent = (JaceEvent) o;

        if (eventTs != jaceEvent.eventTs) {
            return false;
        }
        if (timeBucket != null ? !timeBucket.equals(jaceEvent.timeBucket) : jaceEvent.timeBucket != null) {
            return false;
        }
        if (assetUid != null ? !assetUid.equals(jaceEvent.assetUid) : jaceEvent.assetUid != null) {
            return false;
        }
        if (locUid != null ? !locUid.equals(jaceEvent.locUid) : jaceEvent.locUid != null) {
            return false;
        }
        if (eventType != null ? !eventType.equals(jaceEvent.eventType) : jaceEvent.eventType != null) {
            return false;
        }
        if (zonedDateTime != null ? !zonedDateTime.equals(jaceEvent.zonedDateTime) : jaceEvent.zonedDateTime != null) {
            return false;
        }
        if (measures != null ? !measures.equals(jaceEvent.measures) : jaceEvent.measures != null) {
            return false;
        }
        return tags != null ? tags.equals(jaceEvent.tags) : jaceEvent.tags == null;

    }

    @Override
    public int hashCode() {
        int result = timeBucket != null ? timeBucket.hashCode() : 0;
        result = 31 * result + (assetUid != null ? assetUid.hashCode() : 0);
        result = 31 * result + (locUid != null ? locUid.hashCode() : 0);
        result = 31 * result + (eventType != null ? eventType.hashCode() : 0);
        result = 31 * result + (int) (eventTs ^ (eventTs >>> 32));
        result = 31 * result + (zonedDateTime != null ? zonedDateTime.hashCode() : 0);
        result = 31 * result + (measures != null ? measures.hashCode() : 0);
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        return result;
    }
}
