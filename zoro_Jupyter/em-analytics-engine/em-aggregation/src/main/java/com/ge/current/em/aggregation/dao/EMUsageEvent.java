package com.ge.current.em.aggregation.dao;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by gopalarajurudraraju on 12/12/16.
 */
public class EMUsageEvent implements Serializable {

  private static final long serialVersionUID = 1L;

  // Unique id of the event
  private String id;
  private String entitySourceKey;
  // Simulated data will use this format entitySourceKey_SIM<n>
  // others will simply copy entitySourceKey
  private String gtSourceKey;
  //date and time format should be  yyyy-MM-dd'T'HH:mm:ss.SSSXXX
  // Example : 2016-09-29T13:50:00-05:00
  private String startTime;
  //For energy usage should be KILOWATT or KILOWATTS_PER_HOUR
  private String usageUOM;
  private Double usageAmount;
  //duration data in seconds
  private Number duration;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getEntitySourceKey() {
    return entitySourceKey;
  }

  public void setEntitySourceKey(String entitySourceKey) {
    this.entitySourceKey = entitySourceKey;
  }

  public String getGtSourceKey() {
    return gtSourceKey;
  }

  public void setGtSourceKey(String gtSourceKey) {
    this.gtSourceKey = gtSourceKey;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getUsageUOM() {
    return usageUOM;
  }

  public void setUsageUOM(String usageUOM) {
    this.usageUOM = usageUOM;
  }

  public Double getUsageAmount() {
    return usageAmount;
  }

  public void setUsageAmount(Double usageAmount) {
    this.usageAmount = usageAmount;
  }

  public Number getDuration() {
    return duration;
  }

  public void setDuration(Number duration) {
    this.duration = duration;
  }

  @Override
  public String toString() {
    return "EMUsageEvent{" + "id='" + id + '\'' + ", entitySourceKey='" + entitySourceKey + '\'' + ", gtSourceKey='"
        + gtSourceKey + '\'' + ", startTime='" + startTime + '\'' + ", usageUOM='" + usageUOM + '\'' + ", usageAmount="
        + usageAmount + ", duration=" + duration + '}';
  }
}
