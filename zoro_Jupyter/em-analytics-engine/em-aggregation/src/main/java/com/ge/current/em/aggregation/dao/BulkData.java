package com.ge.current.em.aggregation.dao;

import java.io.Serializable;
import java.util.List;

/**
 * Created by gopalarajurudraraju on 12/12/16.
 */
public class BulkData implements Serializable {

  private static final long serialVersionUID = 1L;
  private String dataSourceType;
  private List<EMUsageEvent> emUsageEvents;

  public String getDataSourceType() {
    return dataSourceType;
  }

  public void setDataSourceType(String dataSourceType) {
    this.dataSourceType = dataSourceType;
  }

  public List<EMUsageEvent> getEmUsageEvents() {
    return emUsageEvents;
  }

  public void setEmUsageEvents(List<EMUsageEvent> emUsageEvents) {
    this.emUsageEvents = emUsageEvents;
  }

  @Override
  public String toString() {
    return "BulkData{" + "dataSourceType='" + dataSourceType + '\'' + ", emUsageEvents=" + emUsageEvents + '}';
  }
}
