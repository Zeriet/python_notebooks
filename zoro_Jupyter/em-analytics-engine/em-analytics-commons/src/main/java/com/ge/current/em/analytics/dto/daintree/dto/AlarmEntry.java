package com.ge.current.em.analytics.dto.daintree.dto;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.daintreenetworks.haystack.commons.model.alarm.AlarmAction;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AlarmEntry implements Serializable, Comparable<AlarmEntry> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3476791300706223153L;
	private static final String CSM_ALARM_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final org.joda.time.format.DateTimeFormatter DTF = org.joda.time.format.DateTimeFormat
			.forPattern(CSM_ALARM_DATE_TIME_FORMAT).withZoneUTC();

	@JsonProperty("timeRef")
	private String timeRef;
	@JsonProperty("facility")
	private String facilityID;
	@JsonProperty("wac")
	private String wacID;
	@JsonProperty("zone")
	private String zoneID;
	@JsonProperty("device")
	private String deviceID;

	private AlarmAction action;
	private Integer alarmId;
	
	@JsonIgnore
	private DateTime ts;

	public AlarmEntry() {

	}

	public String getTimeRef() {
		return timeRef;
	}

	public AlarmEntry setTimeRef(String timeRef) {
		try {
			ts = DTF.parseDateTime(timeRef);
		}catch (Exception ex) {
			System.out.println("Failed to parse:" + timeRef + ", " + ex.getMessage());
		}
		
		this.timeRef = timeRef;
		return this;
	}

	public String getFacilityID() {
		return facilityID;
	}

	public AlarmEntry setFacilityID(String facilityID) {
		this.facilityID = facilityID;
		return this;
	}

	public String getWacID() {
		return wacID;
	}

	public AlarmEntry setWacID(String wacID) {
		this.wacID = wacID;
		return this;
	}

	public String getZoneID() {
		return zoneID;
	}

	public AlarmEntry setZoneID(String zoneID) {
		this.zoneID = zoneID;
		return this;
	}

	public String getDeviceID() {
		return deviceID;
	}

	public AlarmEntry setDeviceID(String deviceID) {
		this.deviceID = deviceID;
		return this;
	}

	public AlarmAction getAction() {
		return action;
	}

	public AlarmEntry setAction(AlarmAction action) {
		this.action = action;
		return this;
	}

	public Integer getAlarmId() {
		return alarmId;
	}

	public AlarmEntry setAlarmId(Integer alarmId) {
		this.alarmId = alarmId;
		return this;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	// optional alarm parameters
	private final Map<String, Object> params = new HashMap<String, Object>();
	
	public DateTime getTs() {
		return ts;
	}

	@Override
	public int compareTo(AlarmEntry o) {
		if (this.ts != null && o.ts != null) {
			return ts.compareTo(o.ts);
		}
		return 0;
	}

}
