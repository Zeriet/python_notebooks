package com.ge.current.em.analytics.dto;

import java.io.Serializable;
import java.util.Date;

import com.daintreenetworks.haystack.commons.model.alarm.AlarmAction;

public class SiteOfflineReadingDTO implements Serializable, Comparable<SiteOfflineReadingDTO>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5743591925346805965L;
	private String edgeDeviceId;
	private String timeStamp;
	private Date utcTimeStamp;
	private Date localTimeStamp;
	private AlarmAction action;
	
	private String siteUid;
	
	public SiteOfflineReadingDTO() {
		
	}

	public String getEdgeDeviceId() {
		return edgeDeviceId;
	}

	public SiteOfflineReadingDTO setEdgeDeviceId(String edgeDeviceId) {
		this.edgeDeviceId = edgeDeviceId;
		return this;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public SiteOfflineReadingDTO setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
		return this;
	}

	public AlarmAction getAction() {
		return action;
	}

	public SiteOfflineReadingDTO setAction(AlarmAction action) {
		this.action = action;
		return this;
	}

	public Date getUtcTimeStamp() {
		return utcTimeStamp;
	}

	public SiteOfflineReadingDTO setUtcTimeStamp(Date utcTimeStamp) {
		this.utcTimeStamp = utcTimeStamp;
		return this;
	}

	public Date getLocalTimeStamp() {
		return localTimeStamp;
	}

	public SiteOfflineReadingDTO setLocalTimeStamp(final Date localTimeStamp) {
		this.localTimeStamp = localTimeStamp;
		return this;
	}
	
	public boolean equals(final Object obj) {
		if (obj instanceof SiteOfflineReadingDTO) {
			final SiteOfflineReadingDTO dto = (SiteOfflineReadingDTO) obj;
			return (siteUid != null ? siteUid.equals(((SiteOfflineReadingDTO) obj).getSiteUid()) : dto.getEdgeDeviceId().equals(this.getEdgeDeviceId())) &&
					 dto.getTimeStamp().equals(this.getTimeStamp()) && dto.getAction().equals(this.getAction());
		}
		return false;
	}
	
	public int hashCode() {
		return (siteUid == null ? this.getEdgeDeviceId().hashCode() : siteUid.hashCode()) + this.getTimeStamp().hashCode() + this.getAction().hashCode();
	}

	@Override
	public int compareTo(SiteOfflineReadingDTO o) {
		return this.getUtcTimeStamp().compareTo(o.getUtcTimeStamp());
	}

	public String getSiteUid() {
		return siteUid;
	}

	public SiteOfflineReadingDTO setSiteUid(String siteUid) {
		this.siteUid = siteUid;
		return this;
	}
}
