package com.ge.current.ie.analytics.cache;

import java.util.Map;

import com.ge.current.em.analytics.common.APMDataLoader;

public class DbCacheServiceImpl implements ApmCacheService{

	transient APMDataLoader apmDataLoader;
	public DbCacheServiceImpl(final APMDataLoader apmDataLoader) {
		this.apmDataLoader = apmDataLoader;
	}
	
	@Override
	public Map<String, String> getResourceNameMapping() {
		return apmDataLoader.getResourceNameMapping();
	}

	@Override
	public Map<String, String> getGatewaySiteMapping() {
		return apmDataLoader.getGatewaySiteMapping();
	}

	@Override
	public Map<String, String> generateSiteTimeZoneMapping() {
		return apmDataLoader.generateSiteTimeZoneMapping();
	}

	@Override
	public Map<String, String> generateSiteEnterpriseMapping() {
		return apmDataLoader.generateSiteEnterpriseMapping();
	}

}
