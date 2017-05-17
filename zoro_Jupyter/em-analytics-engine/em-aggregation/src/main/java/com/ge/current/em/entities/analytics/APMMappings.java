package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class APMMappings implements Serializable {
	HashSet<String> loadTypeSegments;
	Map<String, Double> siteSqftMapping;
	Map<String, HashMap<String, Integer[]>> siteOccupancyMapping;
	Map<String, String> siteTimeZoneMapping;
	Map<String, Integer> siteInstallationYearsMapping;
	Map<String, HashMap<Integer, String>> sitePeakHoursMapping;
	Map<String, String> segmentSiteMapping;
	Map<String, String> segmentEnterpriseMapping;
	Map<String, String> siteEnterpriseMapping;
	Map<String, String> loadTypeSegmentNameMapping;

	public APMMappings() {
		super();
	}

	public APMMappings(HashSet<String> loadTypeSegments, Map<String, Double> siteSqftMapping,
			Map<String, HashMap<String, Integer[]>> siteOccupancyMapping, Map<String, String> siteTimeZoneMapping,
			Map<String, Integer> siteInstallationYearsMapping,
			Map<String, HashMap<Integer, String>> sitePeakHoursMapping, Map<String, String> segmentSiteMapping,
			Map<String, String> segmentEnterpriseMapping, Map<String, String> siteEnterpriseMapping,
			Map<String, String> loadTypeSegmentNameMapping) {
		super();
		this.loadTypeSegments = loadTypeSegments;
		this.siteSqftMapping = siteSqftMapping;
		this.siteOccupancyMapping = siteOccupancyMapping;
		this.siteTimeZoneMapping = siteTimeZoneMapping;
		this.siteInstallationYearsMapping = siteInstallationYearsMapping;
		this.sitePeakHoursMapping = sitePeakHoursMapping;
		this.segmentSiteMapping = segmentSiteMapping;
		this.segmentEnterpriseMapping = segmentEnterpriseMapping;
		this.siteEnterpriseMapping = siteEnterpriseMapping;
		this.loadTypeSegmentNameMapping = loadTypeSegmentNameMapping;
	}

	public HashSet<String> getLoadTypeSegments() {
		return loadTypeSegments;
	}

	public void setLoadTypeSegments(HashSet<String> loadTypeSegments) {
		this.loadTypeSegments = loadTypeSegments;
	}

	public Map<String, Double> getSiteSqftMapping() {
		return siteSqftMapping;
	}

	public void setSiteSqftMapping(Map<String, Double> siteSqftMapping) {
		this.siteSqftMapping = siteSqftMapping;
	}

	public Map<String, HashMap<String, Integer[]>> getSiteOccupancyMapping() {
		return siteOccupancyMapping;
	}

	public void setSiteOccupancyMapping(Map<String, HashMap<String, Integer[]>> siteOccupancyMapping) {
		this.siteOccupancyMapping = siteOccupancyMapping;
	}

	public Map<String, String> getSiteTimeZoneMapping() {
		return siteTimeZoneMapping;
	}

	public void setSiteTimeZoneMapping(Map<String, String> siteTimeZoneMapping) {
		this.siteTimeZoneMapping = siteTimeZoneMapping;
	}

	public Map<String, Integer> getSiteInstallationYearsMapping() {
		return siteInstallationYearsMapping;
	}

	public void setSiteInstallationYearsMapping(Map<String, Integer> siteInstallationYearsMapping) {
		this.siteInstallationYearsMapping = siteInstallationYearsMapping;
	}

	public Map<String, HashMap<Integer, String>> getSitePeakHoursMapping() {
		return sitePeakHoursMapping;
	}

	public void setSitePeakHoursMapping(Map<String, HashMap<Integer, String>> sitePeakHoursMapping) {
		this.sitePeakHoursMapping = sitePeakHoursMapping;
	}

	public Map<String, String> getSegmentSiteMapping() {
		return segmentSiteMapping;
	}

	public void setSegmentSiteMapping(Map<String, String> segmentSiteMapping) {
		this.segmentSiteMapping = segmentSiteMapping;
	}

	public Map<String, String> getLoadTypeSegmentNameMapping() {
		return loadTypeSegmentNameMapping;
	}

	public void setLoadTypeSegmentNameMapping(Map<String, String> loadTypeSegmentNameMapping) {
		this.loadTypeSegmentNameMapping = loadTypeSegmentNameMapping;
	}

	public Map<String, String> getSegmentEnterpriseMapping() {
		return segmentEnterpriseMapping;
	}

	public void setSegmentEnterpriseMapping(Map<String, String> segmentEnterpriseMapping) {
		this.segmentEnterpriseMapping = segmentEnterpriseMapping;
	}

	public Map<String, String> getSiteEnterpriseMapping() {
		return siteEnterpriseMapping;
	}

	public void setSiteEnterpriseMapping(Map<String, String> siteEnterpriseMapping) {
		this.siteEnterpriseMapping = siteEnterpriseMapping;
	}

	@Override
	public String toString() {
		return "APMMappings [loadTypeSegments=" + loadTypeSegments + ", siteSqftMapping=" + siteSqftMapping
				+ ", siteOccupancyMapping=" + siteOccupancyMapping + ", siteTimeZoneMapping=" + siteTimeZoneMapping
				+ ", siteInstallationYearsMapping=" + siteInstallationYearsMapping + ", sitePeakHoursMapping="
				+ sitePeakHoursMapping + ", segmentSiteMapping=" + segmentSiteMapping + ", segmentEnterpriseMapping="
				+ segmentEnterpriseMapping + ", siteEnterpriseMapping=" + siteEnterpriseMapping
				+ ", loadTypeSegmentNameMapping=" + loadTypeSegmentNameMapping + "]";
	}

}