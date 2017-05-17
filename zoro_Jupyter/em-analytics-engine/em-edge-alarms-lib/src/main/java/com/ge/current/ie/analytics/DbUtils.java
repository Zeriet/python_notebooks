package com.ge.current.ie.analytics;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.edgealarms.parser.AlarmConstants;
import com.ge.current.em.entities.analytics.AlertLog;
import com.ge.current.em.entities.analytics.AlertLogStg;
import com.ge.current.em.util.QueryUtil;

public class DbUtils implements DbService {
	public static final String alertReturnParams = "time_bucket, resrc_uid, resrc_type,enterprise_uid, alert_ts, alert_ts_tz, log_uuid,  alert_type, alert_name,severity, category,duration, status, site_uid, site_name,zone_name,asset_uid,asset_type,ext_props";

	private static DbProperties prop;

	// Cassandra Connection info
	private static volatile Cluster cassandraCluster = null;
	private static volatile Session cassandraSession = null;
	private static volatile PreparedStatement preparedStatement;
	private static volatile PreparedStatement preparedStatementStg;
	private static volatile String insertQuery = "INSERT INTO %s.%s (" + DbUtils.alertReturnParams + ") "
			+ "VALUES (?, ?, ?, ?, ?, ?, ? , ? , ? , ? , ? , ? , ? , ?, ?, ?, ?, ?, ?)";

	public DbUtils(DbProperties p) {
		prop = p;
	}

	/**
	 * Cassandra Connection Utility methods. Need to be threadsafe since we have
	 * a multithreaded event processor
	 */

	private static Cluster createClusterWithoutUser(DbProperties propertiesBean) {
		return Cluster.builder().addContactPoints(propertiesBean.getContactPoints().split(","))
				.withPort(Integer.valueOf(propertiesBean.getPort())).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).build();
	}

	private static Cluster createClusterWithUser(final DbProperties propertiesBean, final AuthProvider authProvider) {
		return Cluster.builder().addContactPoints(propertiesBean.getContactPoints().split(","))
				.withPort(Integer.valueOf(propertiesBean.getPort())).withAuthProvider(authProvider)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).build();
	}

	public static void cassandraInit(DbProperties propertiesBean) {
		// Cassandra init
		if (cassandraCluster == null || cassandraCluster.isClosed()) {
			synchronized (DbUtils.class) {
				AuthProvider authProvider = null;
				if (propertiesBean.getUser() != null && !propertiesBean.getUser().isEmpty()) {
					authProvider = new PlainTextAuthProvider(propertiesBean.getUser(), propertiesBean.getPassword());
				}
				if (cassandraCluster == null || cassandraCluster.isClosed()) {
					cassandraCluster = authProvider == null ? createClusterWithoutUser(propertiesBean)
							: createClusterWithUser(propertiesBean, authProvider);
					System.out.println("Successfully created Cassandra Cluster");
				}
			}
		}

		if (cassandraSession == null || cassandraSession.isClosed()) {
			synchronized (DbUtils.class) {
				if (cassandraSession == null || cassandraSession.isClosed()) {
					cassandraSession = cassandraCluster.connect(propertiesBean.getKeyspace());
					System.out.println("Successfully created Cassandra Session");
				}
			}
		}

		if (preparedStatement == null) {
			if (preparedStatement == null) {
				String formattedQuery = String.format(insertQuery, propertiesBean.getKeyspace(), "alert_log");
				preparedStatement = cassandraSession.prepare(formattedQuery);
				preparedStatementStg = cassandraSession.prepare(
						String.format(insertQuery, propertiesBean.getKeyspace(), propertiesBean.getStagingTable()));
			}

		}

	}

	public AlertLog findLastAlertLog(String timeBucket, final String siteId, final String timeZone) {
		cassandraInit(prop);
		final List<String> buckets = getTimeBuckets(timeZone, timeBucket);
		return findLastAlertLog(timeBucket, siteId, buckets, 0);
	}

	private List<AlertLog> readFromCassandra(final String bucket, final String resourceId) {

		// Assuming the it's order by time desc.
		final String query = "SELECT " + alertReturnParams + " FROM " + prop.getKeyspace() + "."
				+ prop.getStagingTable() + " WHERE time_bucket in ('" + bucket + "') and alert_name ='"
				+ AlarmConstants.ALERT_NAME + "' and resrc_uid = '" + resourceId + "'";
		ResultSet results = cassandraSession.execute(query);
		final List<AlertLog> logs = new ArrayList<>();
		for (Row row : results) {
			logs.add(readAlertLogFromDb(row));
		}
		return logs;
	}

	private List<String> getTimeBuckets(String timeZone, String startTime) {
		List<String> timeBuckets = new ArrayList<>();
		final long ONE_MINUTE_IN_MILLIS = 60000;
		SimpleDateFormat formattter = new SimpleDateFormat("yyyyMMddHHmm");
		Date currentDate = new Date();
		try {
			currentDate = formattter.parse(startTime);
		} catch (ParseException ex) {
			System.err.println("Date can not be parsed:" + ex.getMessage());
		}
		long currentTimeInMs = currentDate.getTime();
		int timeInterval = 15;
		for (int i = 1; i <= prop.getAlarmBuckets(); i += 1) {

			Date afterAddingMins = new Date(currentTimeInMs - ((timeInterval * i) * ONE_MINUTE_IN_MILLIS));
			timeBuckets.add(formattter.format(afterAddingMins));
		}
		return timeBuckets;
	}

	public static AlertLog readAlertLogFromDb(final Row cassandraRow) {
		final AlertLog alertLog = new AlertLog();
		alertLog.setResrc_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_RESRC_UID_INDEX));
		alertLog.setDuration(cassandraRow.getInt(QueryUtil.ALERT_LOG_DURATION_INDEX));
		alertLog.setStatus(cassandraRow.getString(QueryUtil.ALERT_LOG_STATUS_INDEX));
		alertLog.setTime_bucket(cassandraRow.getString(QueryUtil.ALERT_LOG_TIME_BUCKET));
		final Object tsValue = cassandraRow.getObject(QueryUtil.ALERT_LOG_ALERT_TS_INDEX);
		if (tsValue != null) {
			try {

				alertLog.setAlert_ts((Date) tsValue);
			} catch (Exception ex) {
				System.err.println("Error parsing tsValue:" + ex.getMessage());
			}
		}
		alertLog.setResrc_type(cassandraRow.getString(QueryUtil.ALERT_LOG_RESC_TYPE_INDEX));
		alertLog.setSite_name(cassandraRow.getString(QueryUtil.ALERT_LOG_SITE_NAME_INDEX));
		alertLog.setZone_name(cassandraRow.getString(QueryUtil.ALERT_LOG_ZONE_NAME_INDEX));
		alertLog.setAsset_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_RESRC_UID_INDEX));
		alertLog.setSite_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_SITE_UID_INDEX));
		alertLog.setAlert_type(cassandraRow.getString(QueryUtil.ALERT_LOG_ALERT_TYPE_INDEX));
		alertLog.setAlert_name(cassandraRow.getString(QueryUtil.ALERT_LOG_ALERT_NAME_INDEX));
		alertLog.setEnterprise_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_ENTERPRISE_UID_INDEX));
		alertLog.setAlert_ts_tz(cassandraRow.getString(QueryUtil.ALERT_LOG_ALERT_TZ_LOC_INDEX));
		alertLog.setTime_bucket(cassandraRow.getString(QueryUtil.ALERT_LOG_TIME_BUCKET));
		alertLog.setStatus(cassandraRow.getString(QueryUtil.ALERT_LOG_STATUS_INDEX));
		alertLog.setSeverity(cassandraRow.getString(QueryUtil.ALERT_LOG_SEVERITY_INDEX));
		alertLog.setCategory(cassandraRow.getString(QueryUtil.ALERT_LOG_CATEGORY_INDEX));
		alertLog.setAsset_uid(cassandraRow.getString(QueryUtil.ALERT_LOG_ASSET_UID_INDEX));
		alertLog.setAsset_type(cassandraRow.getString(QueryUtil.ALERT_LOG_ASSET_TYPE_INDEX));
		alertLog.setLog_uuid(cassandraRow.getUUID(QueryUtil.ALERT_LOG_LOG_UUID_INDEX));
		final Object extProp = cassandraRow.getObject(QueryUtil.ALERT_LOG_EXT_PROPS_INDEX);
		if (extProp != null) {
			HashMap<String, String> extProps = new HashMap<>(
					cassandraRow.getMap(QueryUtil.ALERT_LOG_EXT_PROPS_INDEX, String.class, String.class));
			if (extProps != null) {
				alertLog.setExt_props(extProps);
			}
		}
		return alertLog;
	}

	private AlertLog findLastAlertLog(final String bucket, final String resourceId, final List<String> buckets,
			final int count) {
		if (count < buckets.size()) {
			final List<AlertLog> list = readFromCassandra(bucket, resourceId);
			final int totalFound = list.size();

			if (totalFound == 0) {

				// Find next bucket.
				return findLastAlertLog(buckets.get(count), resourceId, buckets, count + 1);
			}

			// Assume always return order by time_ts desc, just get the last one
			final AlertLog lastAlarm = list.get(0);
			final AlertLog lastActive = lastAlarm.getStatus().equals(AlarmConstants.ALERT_STATUS_ACTIVE)
					|| lastAlarm.getStatus().equals(AlarmConstants.ALERT_STATUS_PAST) ? lastAlarm : null;

			System.out.println("Total " + list.size() + " lastActive:" + ReflectionToStringBuilder.toString(lastActive));

			return lastActive;

		} else {
			// End of the alert Log and still can't find it
			return null;
		}
	}

	public static void printLog(final AlertLog reading) {
		System.out.println(
				"Log:" + reading.getTime_bucket() + ", " + reading.getAlert_ts_loc() + ", " + reading.getResrc_uid()
						+ ", " + reading.getSite_name() + ", " + reading.getStatus() + ", " + reading.getDuration());

	}

	public static AlertLogStg generateAlertLogStag(AlertLog alert) throws Exception {
		AlertLogStg alertLogStg = new AlertLogStg();
		Map<String, String> extProps = new HashMap<>();
		if (alert.getExt_props() != null) {
			alert.getExt_props().entrySet().stream().forEach(x -> extProps.put(x.getKey(), x.getValue()));
		}

		alertLogStg.setDuration(alert.getDuration());
		alertLogStg.setAlert_name(alert.getAlert_name());
		alertLogStg.setAlert_type(alert.getAlert_type());
		alertLogStg.setCategory(alert.getCategory());
		alertLogStg.setSeverity(String.valueOf(alert.getSeverity()));

		alertLogStg.setAlert_ts_tz(alert.getAlert_ts_tz());
		alertLogStg.setAlert_ts(alert.getAlert_ts());
		if (alert.getSite_name() != null) {
			alertLogStg.setSite_name(alert.getSite_name());
		}
		if (alert.getSite_uid() != null) {
			alertLogStg.setSite_uid(alert.getSite_uid());
		}
		if (alert.getZone_name() != null) {
			alertLogStg.setZone_name(alert.getZone_name());
		}
		alertLogStg.setStatus(alert.getStatus());
		alertLogStg.setLog_uuid(UUIDs.timeBased());
		alertLogStg.setEnterprise_uid(alert.getEnterprise_uid());
		alertLogStg.setTime_bucket(alert.getTime_bucket());
		alertLogStg.setResrc_uid(alert.getResrc_uid());
		alertLogStg.setResrc_type(alert.getResrc_type());
		alertLogStg.setExt_props(extProps);
		printLog(alert);
		return alertLogStg;
	}

	public static void saveLogs(final List<AlertLog> logs) throws Exception {
		System.out.println("Saving total:" + logs.size());
		BoundStatement boundStatement = new BoundStatement(preparedStatement);
		BoundStatement boundStatementStg = new BoundStatement(preparedStatementStg);

		// "time_bucket, resrc_uid, resrc_type,enterprise_uid, alert_ts,
		// alert_ts_tz, log_uuid, alert_type, alert_name,severity,
		// category,duration, status, site_uid,
		// site_name,zone_name,asset_uid,asset_type,ext_props"
		for (AlertLog log : logs) {
			System.out.println("Saving:" + ReflectionToStringBuilder.toString(log));
			cassandraSession.execute(boundStatement.bind(log.getTime_bucket(), log.getResrc_uid(), log.getResrc_type(),
					log.getEnterprise_uid(), log.getAlert_ts(), log.getAlert_ts_tz(), log.getLog_uuid(),
					log.getAlert_type(), log.getAlert_name(), log.getSeverity(), log.getCategory(), log.getDuration(),
					log.getStatus(), log.getSite_uid(), log.getSite_name(), log.getZone_name(), log.getAsset_uid(),
					log.getAsset_type(), log.getExt_props()));

			final AlertLogStg stag = DbUtils.generateAlertLogStag(log);
			System.out.println("Saving Stag:" + ReflectionToStringBuilder.toString(stag));
			cassandraSession.execute(boundStatementStg.bind(stag.getTime_bucket(), stag.getResrc_uid(),
					stag.getResrc_type(), stag.getEnterprise_uid(), stag.getAlert_ts(), stag.getAlert_ts_tz(),
					stag.getLog_uuid(), stag.getAlert_type(), stag.getAlert_name(), stag.getSeverity(),
					stag.getCategory(), stag.getDuration(), stag.getStatus(), stag.getSite_uid(), stag.getSite_name(),
					stag.getZone_name(), stag.getAsset_uid(), stag.getAsset_type(), stag.getExt_props()));
		}

	}
}
