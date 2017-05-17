package com.ge.current.ie.analytics;

import java.io.Serializable;

public class DbProperties implements Serializable{
	private String contactPoints;
	private Integer port;
	private String user;
	private String password;
	private String keyspace;
	private String stagingTable;
	private Integer alarmBuckets;
	
	public DbProperties() {
		
	}

	public String getContactPoints() {
		return contactPoints;
	}

	public DbProperties setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
		return this;
	}

	public Integer getPort() {
		return port;
	}

	public DbProperties setPort(Integer port) {
		this.port = port;
		return this;
	}

	public String getUser() {
		return user;
	}

	public DbProperties setUser(String user) {
		this.user = user;
		return this;
	}

	public String getPassword() {
		return password;
	}

	public DbProperties setPassword(String password) {
		this.password = password;
		return this;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public DbProperties setKeyspace(String keyspace) {
		this.keyspace = keyspace;
		return this;
	}

	public String getStagingTable() {
		return stagingTable;
	}

	public DbProperties setStagingTable(String stagingTable) {
		this.stagingTable = stagingTable;
		return this;
	}

	public Integer getAlarmBuckets() {
		return alarmBuckets;
	}

	public DbProperties setAlarmBuckets(Integer alarmBuckets) {
		this.alarmBuckets = alarmBuckets;
		return this;
	}
	
	
	
	
}
