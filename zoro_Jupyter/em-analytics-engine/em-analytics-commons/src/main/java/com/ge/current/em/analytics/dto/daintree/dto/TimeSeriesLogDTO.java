/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.em.analytics.dto.daintree.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.ge.current.em.analytics.dto.daintree.attribute.Attribute;

public class TimeSeriesLogDTO {

    @JsonProperty("timeRef")
    private String timeRef;
    @JsonProperty("timeRef_local")
    private String localTimeRef;
    @JsonProperty("facility")
    private String facilityID;
    @JsonProperty("wac")
    private String wacID;
    @JsonProperty("zone")
    private String zoneID;
    @JsonProperty("device")
    private String deviceID;
    
    @JsonProperty("data")
    private List<EventEntry> data;
    
    public String getTimeRef() {
        return timeRef;
    }

    public TimeSeriesLogDTO setTimeRef(String timeRef) {
        this.timeRef = timeRef;
        return this;
    }

    public String getLocalTimeRef() {
        return localTimeRef;
    }

    public TimeSeriesLogDTO setLocalTimeRef(String localTimeRef) {
        this.localTimeRef = localTimeRef;
        return this;
    }

    public String getFacilityID() {
        return facilityID;
    }

    public TimeSeriesLogDTO setFacilityID(String facilityID) {
        this.facilityID = facilityID;
        return this;
    }

    public String getWacID() {
        return wacID;
    }

    public TimeSeriesLogDTO setWacID(String wacID) {
        this.wacID = wacID;
        return this;
    }

    public String getZoneID() {
        return zoneID;
    }

    public TimeSeriesLogDTO setZoneID(String zoneID) {
        this.zoneID = zoneID;
        return this;
    }

    public String getDeviceID() {
        return deviceID;
    }

    public TimeSeriesLogDTO setDeviceID(String deviceID) {
        this.deviceID = deviceID;
        return this;
    }
    
    public List<EventEntry> getData() {
    	return this.data;
    }
    
    public TimeSeriesLogDTO setData(final List<EventEntry> data) {
    	this.data = data;
    	return this;
    }
}
