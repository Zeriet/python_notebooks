package com.ge.current.ie.analytics.cache;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CacheDataMap implements Serializable{
	Map<String, String> data = new HashMap<>();
	
	public CacheDataMap() {
		
	}

	public Map<String, String> getData() {
		return data;
	}

	public void setData(Map<String, String> data) {
		this.data = data;
	}
	
	
}
