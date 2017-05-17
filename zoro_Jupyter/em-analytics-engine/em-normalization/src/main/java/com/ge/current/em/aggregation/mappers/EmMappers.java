package com.ge.current.em.aggregation.mappers;

import java.util.Date;

import org.apache.spark.api.java.function.Function;

import com.ge.current.ie.model.nosql.NormalizedEventLog;
import com.ge.current.em.aggregation.dao.OutOfSyncLog;

import com.ge.current.em.aggregation.utils.DateUtils;

public class EmMappers {
	
	 public static Function<NormalizedEventLog, OutOfSyncLog> mapNormalizedEventLogToOutOfSyncLog = new Function<NormalizedEventLog, OutOfSyncLog>() {

			@Override
			public OutOfSyncLog call(NormalizedEventLog event) throws Exception {
                if (event == null) return null;

				OutOfSyncLog outOfSync = new OutOfSyncLog();
				outOfSync.setEnterprise_uid(event.getEnterprise_uid());
				outOfSync.setEvent_type(event.getEvent_type());
				outOfSync.setResrc_uid(event.getResrc_uid());

                String currentTimeBucket = DateUtils.getTimeBucket(new Date().getTime());
                outOfSync.setTime_bucket(currentTimeBucket.substring(0, 10));

				outOfSync.setYyyymmddhhmm(event.getTime_bucket());
				outOfSync.setResrc_type(event.getResrc_type());
                outOfSync.setResrc_tz(event.getEvent_ts_tz());
				return outOfSync;
			}
		};
}
