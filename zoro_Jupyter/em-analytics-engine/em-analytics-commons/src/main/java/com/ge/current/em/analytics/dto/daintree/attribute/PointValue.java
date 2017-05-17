package com.ge.current.em.analytics.dto.daintree.attribute;import com.ge.current.em.analytics.dto.daintree.dto.EventEntry;


public class PointValue {
	private PointType type;
	private Object value;
	
	/**
	 * Default impl of Event Entry on unknown 
	 * */
	public PointValue(final EventEntry entry) {
		this.type = PointType.Enum;
		this.value = entry.getTextValue();
	}
	
	public PointValue(final PointType type, final Object value) {
		this.type = type;
		this.value = value;
	}
	
	public PointType getType() {
		return type;
	}
	
	public Object getValue() {
		return value;
	}
	
}
