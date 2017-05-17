package com.ge.current.ie.analytics.cache;

import java.io.Serializable;

public class CacheData implements Serializable{
	private CacheDataMap gatewaySite = new CacheDataMap();
	private CacheDataMap siteTimeZone = new CacheDataMap();
	private CacheDataMap resourceName = new CacheDataMap();
	private CacheDataMap siteEnterprise = new CacheDataMap();
	
	public CacheData() {
		
	}

	public CacheDataMap getGatewaySite() {
		return gatewaySite;
	}

	public void setGatewaySite(final CacheDataMap gatewaySite) {
		this.gatewaySite = gatewaySite;
	}

	public CacheDataMap getSiteTimeZone() {
		return siteTimeZone;
	}

	public void setSiteTimeZone(final CacheDataMap siteTimeZone) {
		this.siteTimeZone = siteTimeZone;
	}

	public CacheDataMap getResourceName() {
		return resourceName;
	}

	public void setResourceName(final CacheDataMap resourceName) {
		this.resourceName = resourceName;
	}

	public CacheDataMap getSiteEnterprise() {
		return siteEnterprise;
	}

	public void setSiteEnterprise(final CacheDataMap siteEnterprise) {
		this.siteEnterprise = siteEnterprise;
	}
}
