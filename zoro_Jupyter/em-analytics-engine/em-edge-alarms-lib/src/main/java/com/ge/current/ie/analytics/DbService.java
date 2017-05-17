package com.ge.current.ie.analytics;

import com.ge.current.em.entities.analytics.AlertLog;
public interface DbService {

	public AlertLog findLastAlertLog(final String bucket, final String resourceId, final String timeZone);
}
