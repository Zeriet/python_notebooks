package com.ge.current.ie.analytics.cache;

import java.io.Serializable;
import java.util.Map;

public interface ApmCacheService extends Serializable {

	public Map<String, String> getResourceNameMapping();

	public Map<String, String> getGatewaySiteMapping();

	public Map<String, String> generateSiteTimeZoneMapping();

	public Map<String, String> generateSiteEnterpriseMapping();

}
