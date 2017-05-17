package com.ge.current.em.analytics.dto;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Date;

import com.daintreenetworks.haystack.commons.model.alarm.AlarmAction;

public class AlertReading implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8582527982902260639L;

	public String siteUid;

	public long eventTs;
	
	public Date eventTsUtc;
	
	public Date eventTsLocal;

	public ZonedDateTime zonedDateTime;
	
	public AlarmAction action;
	
	public AlertReading() {}

	public String getSiteUid() {
		return siteUid;
	}

	public AlertReading setSiteUid(String siteUid) {
		this.siteUid = siteUid;
		return this;
	}

	public long getEventTs() {
		return eventTs;
	}

	public AlertReading setEventTs(long eventTs) {
		this.eventTs = eventTs;
		return this;
	}

	public ZonedDateTime getZonedDateTime() {
		return zonedDateTime;
	}

	public AlertReading setZonedDateTime(ZonedDateTime zonedDateTime) {
		this.zonedDateTime = zonedDateTime;
		return this;
	}

	public AlarmAction getAction() {
		return action;
	}

	public AlertReading setAction(AlarmAction action) {
		this.action = action;
		return this;
	}

	public Date getEventTsUtc() {
		return eventTsUtc;
	}

	public AlertReading setEventTsUtc(final Date eventTsUtc) {
		this.eventTsUtc = eventTsUtc;
		return this;
	}

	public Date getEventTsLocal() {
		return eventTsLocal;
	}

	public AlertReading setEventTsLocal(final Date eventTsLocal) {
		this.eventTsLocal = eventTsLocal;
		return this;
	};
	
	
}
