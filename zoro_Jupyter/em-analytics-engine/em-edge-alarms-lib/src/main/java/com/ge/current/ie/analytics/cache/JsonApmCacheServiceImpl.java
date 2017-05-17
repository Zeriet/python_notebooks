package com.ge.current.ie.analytics.cache;

import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
public class JsonApmCacheServiceImpl implements ApmCacheService{

	private CacheData cacheData;

	public JsonApmCacheServiceImpl(final String jsonPath) {
		// Load CacheData from jsonFile
		System.out.println("Loading cache data from " + jsonPath);
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

		try {
			
			cacheData =objectMapper.readValue(new String(loadData(jsonPath)), CacheData.class);
			System.out.println("Loaded " + cacheData.getGatewaySite().getData().size() + " gatewaySite mapping.");
		} catch (Exception e) {
			System.err.print("Error loading local cache from " + jsonPath + ", ex:" + e.getMessage());
			e.printStackTrace();
			cacheData = new CacheData();
		}

	}
	

	private String loadData(final String jsonPath) {
		try {
			return IOUtils.toString(getClass().getClassLoader().getResourceAsStream(jsonPath));
		} catch (Exception ex) {
			System.err.println("Error loading jsonPath from " + jsonPath);
			ex.printStackTrace();
			return null;
		}
	}
	
	@Override
	public Map<String, String> getResourceNameMapping() {
		return cacheData.getResourceName().getData();
	}

	@Override
	public Map<String, String> getGatewaySiteMapping() {
		return cacheData.getGatewaySite().getData();
	}

	@Override
	public Map<String, String> generateSiteTimeZoneMapping() {
		return cacheData.getSiteTimeZone().getData();
	}

	@Override
	public Map<String, String> generateSiteEnterpriseMapping() {
		return cacheData.getSiteEnterprise().getData();
	}
}
