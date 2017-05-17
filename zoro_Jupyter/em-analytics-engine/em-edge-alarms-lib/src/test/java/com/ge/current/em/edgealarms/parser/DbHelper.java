package com.ge.current.em.edgealarms.parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

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
import com.ge.current.em.entities.analytics.AlertLog;
import com.ge.current.em.entities.analytics.AlertLogStg;
import com.ge.current.ie.analytics.DbProperties;
import com.ge.current.ie.analytics.DbUtils;

public class DbHelper {
	static final DbProperties dbProperties = new DbProperties().setAlarmBuckets(8).setContactPoints("localhost")
			.setKeyspace("iepmaster").setPassword("").setPort(19144).setStagingTable("alert_log_stg").setUser("");

	static Cluster cassandraCluster = null;
	static Session cassandraSession = null;

	private static volatile PreparedStatement preparedStatement;
	private static volatile PreparedStatement preparedStatementStg;
	private static volatile String insertQuery = "INSERT INTO %s.%s (" + DbUtils.alertReturnParams + ") "
			+ "VALUES (?, ?, ?, ?, ?, ?, ? , ? , ? , ? , ? , ? , ? , ?, ?, ?, ?, ?, ?)";
	
	public static DbProperties getProperties() {
		return dbProperties;
	}
	
	public static void prepareDb() throws IOException {
		
		if (cassandraCluster == null || cassandraCluster.isClosed()) {
			AuthProvider authProvider = new PlainTextAuthProvider(dbProperties.getUser(), dbProperties.getPassword());
			if (cassandraCluster == null || cassandraCluster.isClosed()) {
				cassandraCluster = Cluster.builder().addContactPoints(dbProperties.getContactPoints().split(","))
						.withAuthProvider(authProvider).withPort(Integer.valueOf(dbProperties.getPort()))
						.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
						.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy())).build();
			}

		}

		if (cassandraSession == null || cassandraSession.isClosed()) {
			if (cassandraSession == null || cassandraSession.isClosed()) {
				
				Session tmpSession = cassandraCluster.connect();

		        System.out.println("Creating keyspace:" + dbProperties.getKeyspace());
		        try
		        {
		            tmpSession.execute(String.format("DROP KEYSPACE %s", dbProperties.getKeyspace()));
		        }
		        catch (Exception e)
		        {
		            e.printStackTrace();
		        }

		        tmpSession.execute(String.format(
		            "CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
		            dbProperties.getKeyspace()));

		        cassandraSession = cassandraCluster.connect(dbProperties.getKeyspace());
		        
		        final String query = new String(Files.readAllBytes(Paths.get("src/test/resources/schema.cql")));
		        final String[] queries = query.split(";");
		        for (String q : queries) {
		        	if (!q.trim().isEmpty()) {
		        		System.out.println("query:" + q);
			        	cassandraSession.execute(q);
		        	}
		        	
		        }
		        
			}

		}

		if (preparedStatement == null) {
			if (preparedStatement == null) {
				String formattedQuery = String.format(insertQuery, dbProperties.getKeyspace(),
						"alert_log");
				preparedStatement = cassandraSession.prepare(formattedQuery);
				preparedStatementStg = cassandraSession.prepare(String.format(insertQuery, dbProperties.getKeyspace(),
						dbProperties.getStagingTable()));
			}

		}
	}
	
	public static void cleanUp() {
		cassandraSession.execute("truncate alert_log");
		cassandraSession.execute("truncate alert_log_stg");
		System.out.println("Clean up done");
		printLogs(getAllInAlertTable());
	}
	
	

	
	public static void printLogs(final List<AlertLog> logs) {
		System.out.println("Total " + logs.size());
		for (AlertLog log : logs) {
			System.out.println(ReflectionToStringBuilder.toString(log, ToStringStyle.SHORT_PREFIX_STYLE));
		}
	}

	public static List<AlertLog> getAllInAlertTable() {
		final String query = "SELECT " + DbUtils.alertReturnParams + " FROM " + dbProperties.getKeyspace() + "."
				+ dbProperties.getStagingTable();
		ResultSet results = cassandraSession.execute(query);
		final List<AlertLog> logs = new ArrayList<>();
		for (Row row : results) {
			final AlertLog log = DbUtils.readAlertLogFromDb(row);
			logs.add(log);
		}
		return logs;

	}

}
