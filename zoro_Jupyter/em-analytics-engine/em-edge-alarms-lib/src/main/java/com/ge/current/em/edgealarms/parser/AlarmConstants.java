package com.ge.current.em.edgealarms.parser;

import java.util.Arrays;
import java.util.List;

public interface AlarmConstants {
    public static final String ALARM_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX VV";
	public static final String ALERT_NAME = "Site Offline";
	public static final String ALERT_TYPE = "Site Offline";
	public static final String ALERT_CATEGORY = "SITEOFFLINE";
	public static final Integer SEVERITY = 1;
	public static final String RESOURCE_TYPE = "SITE";
	
	
	public static final String ALERT_STATUS_ACTIVE = "active";
	public static final String ALERT_STATUS_PAST = "past";
	public static final String ALERT_STATUS_INACTIVE = "inactive";
	
	// Refer to the list of IDs in https://devcloud.swcoe.ge.com/devspace/display/AOUUL/CSM+Site+Offline+Alarm
	public static final List<Integer> OFFLINE_ALARMS = Arrays.asList(1000, 32770);
	
	
}
