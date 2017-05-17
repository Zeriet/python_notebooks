package com.ge.current.em.aggregation.dao;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;

public class IECommonEvent implements Serializable {

	public String timeBucket;
	public String timeZone;
	public String timeStampPrefix;
	public String timeStampSuffix;

	public String resourceId;
	public String resourceType;
	public String enterpriseId;
	public String siteId;
	public String regionName;
	public String siteCity;
	public String siteState;
	public String siteCountry;
	public String siteName;
	public List<String> zones;
	public List<String> segments;
	public List<String> labels;
    public Date eventTs;

	public Map<String,Double> measures;
	public Map<String, Double> measures_aggr;
	public Map<String, Double> measures_cnt;
	public Map<String, Double> measures_min;
	public Map<String, Double> measures_max;
	public Map<String, Double> measures_avg;

	public Map<String, String> tags ;
	public Map<String, String> ext_props ;

	public IECommonEvent(){
	}

	public IECommonEvent(Map<String , Double> measures){
		this.measures =  new HashMap<>(measures);
		this.measures_aggr =  new HashMap<>(measures);
		this.measures_min =  new HashMap<>(measures);
		this.measures_max =  new HashMap<>(measures);
		this.measures_avg =  new HashMap<>(measures);
		this.measures_cnt =  new HashMap<>(measures);
		this.measures_cnt.keySet().stream().forEach(k ->{
			this.measures_cnt.put(k, 1.0);
		});
	}

	@Override
	public String toString() {
		return "IECommonEvent{" +
				"timeBucket='" + timeBucket + '\'' +
				", timeZone='" + timeZone + '\'' +
				", timeStampPrefix='" + timeStampPrefix + '\'' +
				", timeStampSuffix='" + timeStampSuffix + '\'' +
				", resourceId='" + resourceId + '\'' +
				", resourceType='" + resourceType + '\'' +
				", enterpriseId='" + enterpriseId + '\'' +
				", regionName='" + regionName + '\'' +
				", siteCity='" + siteCity + '\'' +
				", siteState='" + siteState + '\'' +
				", siteCountry='" + siteCountry + '\'' +
				", siteName='" + siteName + '\'' +
				", zones=" + zones +
				", segments=" + segments +
				", labels=" + labels +
				", measures=" + measures +
				", measures_aggr=" + measures_aggr +
				", measures_cnt=" + measures_cnt +
				", measures_min=" + measures_min +
				", measures_max=" + measures_max +
				", measures_avg=" + measures_avg +
				", tags=" + tags +
				", ext_props=" + ext_props +
				'}';
	}

    public IECommonEvent clone(){
        IECommonEvent clone = new IECommonEvent();
        clone.timeBucket = this.timeBucket;
        clone.resourceId = this.resourceId;
        clone.resourceType = this.resourceType;
        clone.siteId = this.siteId;
        clone.enterpriseId = this.enterpriseId;
        clone.eventTs = this.eventTs;
        clone.timeZone = this.timeZone;

        clone.measures = new HashMap<>(this.measures);
        clone.measures_aggr = new HashMap<>(this.measures_aggr);
        clone.measures_cnt = new HashMap<>(this.measures_cnt);
        clone.measures_min = new HashMap<>(this.measures_min);
        clone.measures_max = new HashMap<>(this.measures_max);
        clone.measures_avg = new HashMap<>(this.measures);
        if(this.tags != null)
            clone.tags  = new HashMap<>(this.tags);
        if(this.ext_props != null)
            clone.ext_props  = new HashMap<>(this.ext_props);
        return clone;
    }

}
