package com.ge.current.em.aggregation.utils;

import java.util.Date;

import org.apache.spark.api.java.function.Function;

import com.ge.current.ie.model.nosql.NormalizedEventLog;

public class OutOfSyncUtil {

    public static Function<NormalizedEventLog, Boolean> filterOutOfSyncNormalizedEvents = new Function<NormalizedEventLog, Boolean>() {

		@Override
		public Boolean call(NormalizedEventLog event) throws Exception {
			return OutOfSyncUtil.isOutOfSync(event);
		}
	};
	
	public static boolean isOutOfSync(NormalizedEventLog event){
		String eventTimeBucket = DateUtils.getTimeBucket(event.getEvent_ts().getTime());
		String currentTimeBucket = DateUtils.getTimeBucket(new Date().getTime());
		return ! eventTimeBucket.equalsIgnoreCase(currentTimeBucket);
	}
}
