package com.ge.current.em.aggregation.dao;

import java.io.Serializable;

public class OutOfSyncLog implements Serializable{

	//Key is the yyyyMMddHH
	public String time_bucket;

	public String yyyymmddhhmm;

	public String enterprise_uid;

	public String resrc_uid;

	public String resrc_type;

	public String resrc_tz;

	public String event_type;

	public boolean runForMin;

	public boolean runForHour;

	public boolean runForDay;

	public boolean runForMonth;

	public String solr_query;

	public String getTime_bucket() {
		return time_bucket;
	}

	public void setTime_bucket(String time_bucket) {
		this.time_bucket = time_bucket;
	}

	public String getEnterprise_uid() {
		return enterprise_uid;
	}

	public void setEnterprise_uid(String enterprise_uid) {
		this.enterprise_uid = enterprise_uid;
	}

	public String getResrc_uid() {
		return resrc_uid;
	}

	public void setResrc_uid(String resrc_uid) { this.resrc_uid = resrc_uid; }

	public String getResrc_type() {
		return resrc_type;
	}

	public void setResrc_type(String resrc_type) {
		this.resrc_type = resrc_type;
	}

	public String getEvent_type() {
		return event_type;
	}

	public void setEvent_type(String event_type) {
		this.event_type = event_type;
	}

	public String getYyyymmddhhmm() {
		return yyyymmddhhmm;
	}

	public void setYyyymmddhhmm(String yyyymmddhhmm) {
		this.yyyymmddhhmm = yyyymmddhhmm;
	}

	public boolean isRunForMin() {
		return runForMin;
	}

	public void setRunForMin(boolean runForMin) {
		this.runForMin = runForMin;
	}

	public boolean isRunForHour() {
		return runForHour;
	}

	public void setRunForHour(boolean runForHour) {
		this.runForHour = runForHour;
	}

	public boolean isRunForMonth() {
		return runForMonth;
	}

	public void setRunForMonth(boolean runForMonth) {
		this.runForMonth = runForMonth;
	}

	public boolean isRunForDay() {
		return runForDay;
	}

	public void setRunForDay(boolean runForDay) {
		this.runForDay = runForDay;
	}

	public String getResrc_tz() {
		return resrc_tz;
	}

	public void setResrc_tz(String resrc_tz) {
		this.resrc_tz = resrc_tz;
	}




	@Override
	public String toString() {
		return "OutOfSyncLog [time_bucket=" + time_bucket + ", yyyymmddhhmm=" + yyyymmddhhmm + ", enterprise_uid="
				+ enterprise_uid + ", resrc_uid=" + resrc_uid + ", resrc_type=" + resrc_type + ", resrc_tz=" + resrc_tz
				+ ", event_type=" + event_type + ", runForMin=" + runForMin + ", runForHour=" + runForHour
				+ ", runForDay=" + runForDay + ", runForMonth=" + runForMonth + ", solr_query=" + solr_query + "]";
	}

	public String getSolr_query() { return solr_query; }

	public void setSolr_query(String solr_query) { this.solr_query = solr_query; }


}