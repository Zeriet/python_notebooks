package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class AlertLog implements Serializable {
	
	private String time_bucket ; 
	private String enterprise_uid ; 
	private String resrc_uid ;
	private Date alert_ts ;
	private UUID log_uuid;
	private String	resrc_type ;
	private Date alert_ts_loc;
	private String alert_ts_tz ;
	private String alert_set ;
	private String alert_type ;
	private String alert_name ;
	private String alert_desc ;
	private String severity ;
	private String category ;
	private String scope ;
	private int duration ;
	private String status ;
	private String incident_cnt ;
	private String region_name ;
	private String site_city ;
	private String site_state ;
	private String site_country ;
	private String site_uid ;
	private String site_name ;
	private String zone_uid ;
	private String zone_name ;
	private String segment_uid ;
	private String segment_name ;
	private String segment_type ;
	private String asset_uid ;
	private String asset_type ;
	private String point_uid;
	private String point_type ;
	private String labels ;
	private String rule_uid ;
	private String owner ;
	private String incident_nbr ;
	private Map<String, String> tags ;
	private Map<String, Double> measures ;
	private Map<String, String> ext_props;
	private String solr_query;


	public AlertLog() {

	}

	public AlertLog(AlertLog inputAlertLog) {
		this.time_bucket = inputAlertLog.time_bucket;
		enterprise_uid = inputAlertLog.enterprise_uid;
		resrc_uid = inputAlertLog.resrc_uid;
		 alert_ts = inputAlertLog.alert_ts;
		log_uuid = inputAlertLog.log_uuid;
		resrc_type = resrc_uid;
		alert_ts_loc = inputAlertLog.alert_ts_loc;
		alert_ts_tz = inputAlertLog.alert_ts_tz;
		alert_set = inputAlertLog.alert_set;
		alert_type = inputAlertLog.alert_type;
		alert_name = inputAlertLog.alert_name;
		alert_desc = inputAlertLog.alert_desc;
		severity =inputAlertLog.severity;
		category =inputAlertLog.category;
		scope =inputAlertLog.scope;
		duration =inputAlertLog.duration;
		status =inputAlertLog.status;
		incident_cnt = inputAlertLog.incident_cnt;
		region_name =inputAlertLog.region_name;
		site_city = inputAlertLog.site_city;
		site_state = inputAlertLog.site_state;
		site_country = inputAlertLog.site_country;
		site_uid = inputAlertLog.site_uid;
		site_name = inputAlertLog.site_name;
		zone_uid = inputAlertLog.zone_uid;
		zone_name = inputAlertLog.zone_name;
		segment_uid = inputAlertLog.segment_uid;
		segment_name = inputAlertLog.segment_name;
		segment_type =inputAlertLog.segment_type;
		asset_uid = inputAlertLog.asset_uid;
		asset_type =inputAlertLog.asset_type;
		point_uid = inputAlertLog.point_uid;
		point_type = inputAlertLog.point_type;
		labels = inputAlertLog.labels;
		rule_uid = inputAlertLog.rule_uid;
		owner = inputAlertLog.owner;
		incident_nbr = inputAlertLog.incident_nbr;
		tags = inputAlertLog.tags;
		 measures = inputAlertLog.measures;
		ext_props = inputAlertLog.ext_props;
		solr_query = inputAlertLog.solr_query;

	}
	public String getTime_bucket() {
		return time_bucket;
	}
	public String getEnterprise_uid() {
		return enterprise_uid;
	}
	public String getResrc_uid() {
		return resrc_uid;
	}
	public Date getAlert_ts() {
		return alert_ts;
	}
	public UUID getLog_uuid() {
		return log_uuid;
	}
	public String getResrc_type() {
		return resrc_type;
	}
	public Date getAlert_ts_loc() {
		return alert_ts_loc;
	}
	public String getAlert_set() {
		return alert_set;
	}
	public String getAlert_type() {
		return alert_type;
	}
	public String getAlert_name() {
		return alert_name;
	}
	public String getAlert_desc() {
		return alert_desc;
	}
	public String getSeverity() {
		return severity;
	}
	public String getCategory() {
		return category;
	}
	public String getScope() {
		return scope;
	}
	public int getDuration() {
		return duration;
	}
	public String getStatus() {
		return status;
	}
	public String getIncident_cnt() {
		return incident_cnt;
	}
	public String getRegion_name() {
		return region_name;
	}
	public String getSite_city() {
		return site_city;
	}
	public String getSite_state() {
		return site_state;
	}
	public String getSite_country() {
		return site_country;
	}
	public String getSite_uid() {
		return site_uid;
	}
	public String getSite_name() {
		return site_name;
	}
	public String getZone_uid() {
		return zone_uid;
	}
	public String getZone_name() {
		return zone_name;
	}
	public String getSegment_uid() {
		return segment_uid;
	}
	public String getSegment_name() {
		return segment_name;
	}
	public String getSegment_type() {
		return segment_type;
	}
	public String getAsset_uid() {
		return asset_uid;
	}
	public String getAsset_type() {
		return asset_type;
	}
	public String getPoint_uid() {
		return point_uid;
	}
	public String getPoint_type() {
		return point_type;
	}
	public String getLabels() {
		return labels;
	}
	public String getRule_uid() {
		return rule_uid;
	}
	public String getOwner() {
		return owner;
	}
	public String getIncident_nbr() {
		return incident_nbr;
	}
	public Map<String, String> getTags() {
		return tags;
	}
	public Map<String, Double> getMeasures() {
		return measures;
	}
	public Map<String, String> getExt_props() {
		return ext_props;
	}
	public void setTime_bucket(String time_bucket) {
		this.time_bucket = time_bucket;
	}
	public void setEnterprise_uid(String enterprise_uid) {
		this.enterprise_uid = enterprise_uid;
	}
	public void setResrc_uid(String resrc_uid) {
		this.resrc_uid = resrc_uid;
	}
	public void setAlert_ts(Date alert_ts) {
		this.alert_ts = alert_ts;
	}
	public void setLog_uuid(UUID log_uuid) {
		this.log_uuid = log_uuid;
	}
	public void setResrc_type(String resrc_type) {
		this.resrc_type = resrc_type;
	}
	public void setAlert_ts_loc(Date alert_ts_loc) {
		this.alert_ts_loc = alert_ts_loc;
	}

	public void setAlert_set(String alert_set) {
		this.alert_set = alert_set;
	}
	public void setAlert_type(String alert_type) {
		this.alert_type = alert_type;
	}
	public void setAlert_name(String alert_name) {
		this.alert_name = alert_name;
	}
	public void setAlert_desc(String alert_desc) {
		this.alert_desc = alert_desc;
	}
	public void setSeverity(String severity) {
		this.severity = severity;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public void setScope(String scope) {
		this.scope = scope;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public void setIncident_cnt(String incident_cnt) {
		this.incident_cnt = incident_cnt;
	}
	public void setRegion_name(String region_name) {
		this.region_name = region_name;
	}
	public void setSite_city(String site_city) {
		this.site_city = site_city;
	}
	public void setSite_state(String site_state) {
		this.site_state = site_state;
	}
	public void setSite_country(String site_country) {
		this.site_country = site_country;
	}
	public void setSite_uid(String site_uid) {
		this.site_uid = site_uid;
	}
	public void setSite_name(String site_name) {
		this.site_name = site_name;
	}
	public void setZone_uid(String zone_uid) {
		this.zone_uid = zone_uid;
	}
	public void setZone_name(String zone_name) {
		this.zone_name = zone_name;
	}
	public void setSegment_uid(String segment_uid) {
		this.segment_uid = segment_uid;
	}
	public void setSegment_name(String segment_name) {
		this.segment_name = segment_name;
	}
	public void setSegment_type(String segment_type) {
		this.segment_type = segment_type;
	}
	public void setAsset_uid(String asset_uid) {
		this.asset_uid = asset_uid;
	}
	public void setAsset_type(String asset_type) {
		this.asset_type = asset_type;
	}
	public void setPoint_uid(String point_uid) {
		this.point_uid = point_uid;
	}
	public void setPoint_type(String point_type) {
		this.point_type = point_type;
	}
	public void setLabels(String labels) {
		this.labels = labels;
	}
	public void setRule_uid(String rule_uid) {
		this.rule_uid = rule_uid;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public void setIncident_nbr(String incident_nbr) {
		this.incident_nbr = incident_nbr;
	}
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	public void setMeasures(Map<String, Double> measures) {
		this.measures = measures;
	}
	public void setExt_props(Map<String, String> ext_props) {
		this.ext_props = ext_props;
	}
	public String getAlert_ts_tz() {
		return alert_ts_tz;
	}
	public void setAlert_ts_tz(String alert_ts_tz) {
		this.alert_ts_tz = alert_ts_tz;
	}
	public String getSolr_query() {
		return solr_query;
	}
	public void setSolr_query(String solr_query) {
		this.solr_query = solr_query;
	}

	@Override
	public String toString() {
		return "AlertLog{" + "time_bucket='" + time_bucket + '\'' + ", enterprise_uid='" + enterprise_uid + '\''
				+ ", resrc_uid='" + resrc_uid + '\'' + ", alert_ts=" + alert_ts + ", log_uuid=" + log_uuid
				+ ", resrc_type='" + resrc_type + '\'' + ", alert_ts_loc=" + alert_ts_loc + ", alert_ts_tz='"
				+ alert_ts_tz + '\'' + ", alert_set='" + alert_set + '\'' + ", alert_type='" + alert_type + '\''
				+ ", alert_name='" + alert_name + '\'' + ", alert_desc='" + alert_desc + '\'' + ", severity='"
				+ severity + '\'' + ", category='" + category + '\'' + ", scope='" + scope + '\'' + ", duration="
				+ duration + ", status='" + status + '\'' + ", incident_cnt='" + incident_cnt + '\'' + ", region_name='"
				+ region_name + '\'' + ", site_city='" + site_city + '\'' + ", site_state='" + site_state + '\''
				+ ", site_country='" + site_country + '\'' + ", site_uid='" + site_uid + '\'' + ", site_name='"
				+ site_name + '\'' + ", zone_uid='" + zone_uid + '\'' + ", zone_name='" + zone_name + '\''
				+ ", segment_uid='" + segment_uid + '\'' + ", segment_name='" + segment_name + '\'' + ", segment_type='"
				+ segment_type + '\'' + ", asset_uid='" + asset_uid + '\'' + ", asset_type='" + asset_type + '\''
				+ ", point_uid='" + point_uid + '\'' + ", point_type='" + point_type + '\'' + ", labels='" + labels
				+ '\'' + ", rule_uid='" + rule_uid + '\'' + ", owner='" + owner + '\'' + ", incident_nbr='"
				+ incident_nbr + '\'' + ", tags=" + tags + ", measures=" + measures + ", ext_props=" + ext_props + '}';
	}
}
