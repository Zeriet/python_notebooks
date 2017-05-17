package com.ge.current.em.analytics.dto.daintree.dto;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.ge.current.em.analytics.dto.daintree.attribute.Attribute;

public class EventEntry implements Comparable<EventEntry>{
	public static final String MESSAGE_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
	private static final DateTimeFormatter UTC_FT = DateTimeFormat.forPattern(MESSAGE_DATE_TIME_FORMAT).withZoneUTC();

	@JsonProperty("attribute")
	private String attribute;
	@JsonProperty("value_i")
	private int intValue;
	@JsonProperty("value_t")
	private String textValue;
	@JsonProperty("value_f")
	private float floatValue;
	@JsonProperty("timeRef_local")
	private String timeRefLocal;
	private String timeRef;
	
	@JsonIgnore
	private DateTime utcTs;
	
	@JsonIgnore
	private Attribute attrType;

	public EventEntry() {

	}

	public String getAttribute() {
		return attribute;
	}

	public EventEntry setAttribute(String attribute) {
		this.attribute = attribute;
		this.attrType = Attribute.fromName(attribute);
		return this;
	}

	public int getIntValue() {
		return intValue;
	}

	public EventEntry setIntValue(int intValue) {
		this.intValue = intValue;
		return this;
	}

	public String getTextValue() {
		return textValue;
	}

	public EventEntry setTextValue(String textValue) {
		this.textValue = textValue;
		return this;
	}

	public float getFloatValue() {
		return floatValue;
	}

	public EventEntry setFloatValue(float floatValue) {
		this.floatValue = floatValue;
		return this;
	}

	public Attribute getAttrType() {
		return attrType;
	}

	public String getTimeRefLocal() {
		return timeRefLocal;
	}

	public EventEntry setTimeRefLocal(String timeRefLocal) {
		this.timeRefLocal = timeRefLocal;
		return this;
	}

	public String getTimeRef() {
		return timeRef;
	}

	public EventEntry setTimeRef(String timeRef) {
		this.timeRef = timeRef;
		try {
			utcTs = UTC_FT.parseDateTime(timeRef);
		} catch (Exception ex) {
			System.err.print("UTC Time parser failed:" + ex.getMessage());
		}
		
		return this;
	}
	
	@Override
	public int compareTo(EventEntry o) {
		if (this.utcTs != null && o.utcTs != null) {
			return utcTs.compareTo(o.utcTs);
		}
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
