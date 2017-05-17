package com.ge.current.em.analytics.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import com.ge.current.em.analytics.dto.PointObject;
import com.ge.current.em.persistenceapi.dto.GeoCoordinatesDTO;
import com.ge.current.em.persistenceapi.dto.LocationDTO;
import com.ge.current.em.persistenceapi.dto.MetaEntityDTO;
import com.ge.current.em.persistenceapi.dto.MetaTypeDTO;
import com.ge.current.em.persistenceapi.dto.PropertiesDTO;
import com.ge.current.em.persistenceapi.service.AssetService;
import com.ge.current.em.persistenceapi.service.EnterpriseService;
import com.ge.current.em.persistenceapi.service.SegmentService;
import com.ge.current.em.persistenceapi.service.SiteService;

@Component
public class APMDataLoader {
	private static final Logger logger = Logger.getLogger(APMDataLoader.class);
	private static int testCounter = 0;
	private static int gatewaySiteCounter = 0;

	@Autowired
	EnterpriseService enterpriseService;

	@Autowired
	SiteService siteService;

	@Autowired
	SegmentService segmentService;

	@Autowired
	AssetService assetService;

	@Value(value = "${enterprise.sourcekeys}")
	private String enterpriseSourcekeys;

	Map<String, String> assetEnterpriseMapping = new HashMap<String, String>();
	Map<String, String> assetSiteMapping = new HashMap<String, String>();
	Map<String, Set<String>> assetSegmentMapping = new HashMap<String, Set<String>>();
	Map<String, String> assetTypeMapping = new HashMap<String, String>();
	Map<String, String> gatewaySiteMapping = new HashMap<String, String>();

	Map<String, Map<String, String>> haystackMapping = new HashMap<String, Map<String, String>>();
	Map<String, Map<String, PointObject>> siteHaystackMapping = new HashMap();

	Map<String, String> segmentEnterpriseMapping = new HashMap<String, String>();
	Map<String, String> segmentSiteMapping = new HashMap<String, String>();
	Map<String, String> siteEnterpriseMapping = new HashMap<String, String>();

	Map<String, Double> siteSqftMapping = new HashMap<String, Double>();
	Map<String, String> siteInstallationDatesMapping = new HashMap<String, String>();
	Map<String, String> siteTimeZoneMapping = new HashMap<String, String>();

	Map<String, String> assetTimeZoneMapping = new HashMap<String, String>();

	Map<String, HashMap<String, Integer[]>> siteOccupancyMapping = new HashMap<String, HashMap<String, Integer[]>>();
	Map<String, Integer> siteInstallationYearsMapping = new HashMap<String, Integer>();
	Map<String, HashMap<Integer, String>> sitePeakHoursMapping = new HashMap<String, HashMap<Integer, String>>();
	HashMap<Integer, Integer[]> dummyOccupancyInfo = new HashMap<Integer, Integer[]>();
	Map<String, String> resourceNameMapping = new HashMap<String, String>();

	Map<String, Set<String>> subMeterToSegmentsMapping = new HashMap<String, Set<String>>();
	Map<String, List<String>> loadTypeSegmentMapping = new HashMap();

	// All Submeters Related Mappings
	Map<String, Set<String>> siteToSubmeterMapping = new HashMap();
	Map<String, Set<String>> enterpriseToSubmeterMapping = new HashMap();
	Map<String, Set<String>> segmentToSubmeterMapping = new HashMap();

	// all loadType related mappings
	Map<String, String> loadTypeMapping = new HashMap();
	Map<String, String> loadTypeSegmentNameMapping = new HashMap();

	// all B&R related sites/costmeters/loadtypeSegments list.
	Set<String> billingList = new HashSet();

	// configuration error map
	Map<String, String> configErrorMap = new HashMap();

	public Map<String, String> getLoadTypeMapping() {
		List<String> enterpriseList = null;
		if (enterpriseSourcekeys != null) {
			System.out.println("=== enterpriseSourcekeys= " + enterpriseSourcekeys);

			enterpriseList = Arrays.asList(enterpriseSourcekeys.split(","));
			if (enterpriseSourcekeys.contains("ALL") || enterpriseList.size() == 0) {
				enterpriseList = getAllEnterprises();
			}

		} else {
			System.out.println("=== enterpriseSourcekeys is null =====");
			enterpriseList = getAllEnterprises();
		}

		if (loadTypeMapping.isEmpty()) {
			System.out.println("Calling buildLoadTypeMapping with enterprises: " + enterpriseList);
			buildLoadTypeMapping(enterpriseList);
		}
		return loadTypeMapping;
	}

	public Map<String, Set<String>> getSubMeterToSegmentsMapping() {
		return subMeterToSegmentsMapping;
	}

	public Map<String, Set<String>> getSiteToSubmeterMapping() {
		return siteToSubmeterMapping;
	}

	public Set<String> getBillingList() {
		return billingList;
	}

	public Map<String, Set<String>> getEnterpriseToSubmeterMapping() {
		return enterpriseToSubmeterMapping;
	}

	public Map<String, Set<String>> getSegmentToSubmeterMapping() {
		return segmentToSubmeterMapping;
	}

	public Map<String, Map<String, PointObject>> getSiteHaystackMapping() {
		return siteHaystackMapping;
	}

	public Map<String, String> getResourceNameMapping() {
		return resourceNameMapping;
	}

	public Map<String, String> getGatewaySiteMapping() {
		return gatewaySiteMapping;
	}

	public Map<String, String> getConfigErrorMap() {
		return configErrorMap;
	}

	public void setSubMeterToSegmentsMapping(Map<String, Set<String>> subMeterToSegmentsMapping) {
		this.subMeterToSegmentsMapping = subMeterToSegmentsMapping;
	}

	public Map<String, String> generateAssetEnterpriseMapping() {
		return assetEnterpriseMapping;
	}

	public Map<String, String> generateAssetSiteMapping() {
		return assetSiteMapping;
	}

	public Map<String, Set<String>> generateAssetSegmentMapping() {
		return assetSegmentMapping;
	}

	public Map<String, String> generateAssetTypeMapping() {
		return assetTypeMapping;
	}

	public Map<String, Map<String, String>> generateHaystackMapping() {
		return haystackMapping;
	}

	public Map<String, String> generateSegmentSiteMapping() {
		return segmentSiteMapping;
	}

	public Map<String, String> generateSegmentEnterpriseMapping() {
		return segmentEnterpriseMapping;
	}

	public Map<String, String> generateSiteEnterpriseMapping() {
		return siteEnterpriseMapping;
	}

	public Map<String, Double> generateSiteSqftMapping() {
		return siteSqftMapping;
	}

	public Map<String, String> generateSiteTimeZoneMapping() {
		return siteTimeZoneMapping;
	}

	public Map<String, String> generateAssetTimezoneMapping() {
		return assetTimeZoneMapping;
	}

	public Map<String, HashMap<String, Integer[]>> generateSiteOccupancyMapping() {
		return siteOccupancyMapping;
	}

	public Map<String, Integer> generateSiteInstallationYearMapping() {
		return siteInstallationYearsMapping;
	}

	public Map<String, HashMap<Integer, String>> generateSitePeakHoursMapping() {
		return sitePeakHoursMapping;
	}

	public Map<String, String> getLoadTypeSegmentNameMapping() {
		return loadTypeSegmentNameMapping;
	}

	/*
	 * public String getSiteUid(String resourceUid) { String siteUid = null; if
	 * (assetSiteMapping.isEmpty() || segmentSiteMapping.isEmpty()) {
	 * generateMappings(); } if (resourceUid.contains("ASSET")) { siteUid =
	 * assetSiteMapping.get(resourceUid); } else if
	 * (resourceUid.contains("SEGMENT")) { siteUid =
	 * segmentSiteMapping.get(resourceUid); } else if
	 * (resourceUid.contains("SITE")) { siteUid = resourceUid; }
	 * 
	 * return siteUid; }
	 * 
	 * public String getEnterpriseUid(String resourceUid) { String enterpriseUid
	 * = null; if (assetEnterpriseMapping.isEmpty() ||
	 * segmentEnterpriseMapping.isEmpty() || siteEnterpriseMapping.isEmpty()) {
	 * generateMappings(); } if (resourceUid.contains("ASSET")) { enterpriseUid
	 * = assetEnterpriseMapping.get(resourceUid); } else if
	 * (resourceUid.contains("SEGMENT")) { enterpriseUid =
	 * segmentEnterpriseMapping.get(resourceUid); } else if
	 * (resourceUid.contains("SITE")) { enterpriseUid =
	 * siteEnterpriseMapping.get(resourceUid); } else if
	 * (resourceUid.contains("ENTERPRISE")) { enterpriseUid = resourceUid; }
	 * 
	 * return enterpriseUid; }
	 */

	public void generateMappingForSiteOffline() throws Exception {
		System.out.println("======== Generate All Mappings for SiteOfflineAlarms ==========");
		final long start = System.currentTimeMillis();
		// Page<MetaEntityDTO> enterpriseList =
		// enterpriseService.getRootEnterprises(false, pageRequest);
		List<String> enterpriseList = null;
		System.out.println("generateMappings: enterpriseSourcekeys -> " + enterpriseSourcekeys);
		if (enterpriseSourcekeys != null || enterpriseSourcekeys.length() != 0) {
			System.out.println("=== enterpriseSourcekeys= " + enterpriseSourcekeys);
			enterpriseList = Arrays.asList(enterpriseSourcekeys.split(","));
			if (enterpriseSourcekeys.contains("ALL")) {
				// get all the enterprise sourcekeys
				System.out.println("=== Get All the Enterprises =====");
				enterpriseList = getAllEnterprises();
			}
		} else {
			System.out.println("=== No enterpriseUid configured, Get All the Enterprises =====");
			enterpriseList = getAllEnterprises();
		}

		System.out.println("generateMappings: enterpriseList -> " + enterpriseList);

		if (enterpriseList == null || enterpriseList.size() == 0) {
			System.out.println("========== enterpriseList is null  =============");
			return;
		}
		for (String enterpriseSourceKey : enterpriseList) {
			System.out.println("searching for enterpriseSourceKey: " + enterpriseSourceKey);

			Pageable sitePageRequest = new PageRequest(0, 1000000);
			Page<MetaEntityDTO> siteList = enterpriseService.getEnterpriseSites(enterpriseSourceKey, true,
					sitePageRequest);

			if (siteList == null || siteList.getSize() == 0) {
				System.out.println("========== sizelist is null or 0 size for enterpriseSourceKey ============="
						+ enterpriseSourceKey);
				continue;
			}

			for (MetaEntityDTO site : siteList) {
				String siteSourceKey = site.getSourceKey();

				System.out.println(" !!!!!!!!! site MetaEntityDTO : " + site);

				String siteName = site.getName();
				if (siteName != null) {
					System.out.println(" !!!!!!!!! site Name: " + siteName);
					resourceNameMapping.put(siteSourceKey, siteName);
				}

				System.out.println("========== locations for siteSourceKey =============" + siteSourceKey);

				List<LocationDTO> locations = siteService.getSiteLocations(siteSourceKey);
				String siteTimeZone = null;
				if (locations != null && locations.size() > 0) {
					LocationDTO loc = locations.get(0);
					GeoCoordinatesDTO geoCoordinate = loc.getGeoCoordinates();
					if (geoCoordinate != null) {
						siteTimeZone = geoCoordinate.getTimezone();
					}
				}

				siteTimeZoneMapping.put(siteSourceKey, siteTimeZone);
				siteEnterpriseMapping.put(siteSourceKey, enterpriseSourceKey);
				System.out.println("siteSourceKey: " + siteSourceKey);

				Pageable segmentPageRequest = new PageRequest(0, 1000000);
				MetaEntityDTO siteDescendantObj = siteService.getSiteDescendants(siteSourceKey, 1000, null,
						segmentPageRequest);

				if (siteDescendantObj == null) {
					System.out.println("====== siteDescendantObj is null for ========" + siteSourceKey);
					continue;
				}

				List<MetaEntityDTO> segmentList = siteDescendantObj.getChildren();
				if (segmentList == null || segmentList.size() == 0) {
					System.out
							.println("========== segmentList is null for siteSourceKey =============" + siteSourceKey);
					continue;
				}

				for (MetaEntityDTO segment : segmentList) {
					String segmentSourceKey = segment.getSourceKey();

					List<MetaEntityDTO> assetList = segment.getChildren();
					if (assetList == null || assetList.size() == 0) {
						System.out.println("====assetList is null or 0 size for segmentSourceKey ============="
								+ segmentSourceKey);
						continue;
					}

					for (MetaEntityDTO asset : assetList) {
						String assetSourceKey = asset.getSourceKey();

						String assetName = asset.getName();
						if (assetName != null) {
							System.out.println("*** AssetName = " + assetName);
							resourceNameMapping.put(assetSourceKey, assetName);
						}

						MetaTypeDTO metaType = asset.getMetaType();
						if (metaType == null)
							continue;

						String assetType = metaType.getName();

						// store the gateway to site mapping for the JACE
						// edgeDeviceId for lookup.
						// the gateway will on the SITE level.
						if (assetType.equalsIgnoreCase("GATEWAY")) {
							String extRefId = asset.getExternalRefId();
							System.out.println("->>>>>>>Gateway:" + extRefId + " for " + siteSourceKey);
							gatewaySiteMapping.put(extRefId, siteSourceKey);
						}
					}
				}
			}
			System.out.println("*** End Searching for enterprise: " + enterpriseSourceKey);

		}

		System.out.println("~~~~~~~~~~~~~~~~~~~~~siteEnterpriseMapping: " + siteEnterpriseMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~siteTimeZoneMapping: " + siteTimeZoneMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~gatewaySiteMapping: " + gatewaySiteMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~resourceNameMapping: " + resourceNameMapping);
		System.out.print(
				"~~~~~~~~~~~~~~~~~~~~~Reload Mapping done, took:" + (System.currentTimeMillis() - start) + " ms");
	}

	public void generateMappings() throws Exception {
		System.out.println("======== Generate All Mappings ==========");

		Pageable pageRequest = new PageRequest(0, 500);
		// Page<MetaEntityDTO> enterpriseList =
		// enterpriseService.getRootEnterprises(false, pageRequest);
		List<String> enterpriseList = null;
		System.out.println("generateMappings: enterpriseSourcekeys -> " + enterpriseSourcekeys);
		if (enterpriseSourcekeys != null || enterpriseSourcekeys.length() != 0) {
			System.out.println("=== enterpriseSourcekeys= " + enterpriseSourcekeys);
			enterpriseList = Arrays.asList(enterpriseSourcekeys.split(","));
			if (enterpriseSourcekeys.contains("ALL")) {
				// get all the enterprise sourcekeys
				System.out.println("=== Get All the Enterprises =====");
				enterpriseList = getAllEnterprises();
			}
		} else {
			System.out.println("=== No enterpriseUid configured, Get All the Enterprises =====");
			enterpriseList = getAllEnterprises();
		}

		System.out.println("generateMappings: enterpriseList -> " + enterpriseList);

		buildLoadTypeMapping(enterpriseList);
		buildSubmeterMapping(enterpriseList);

		if (enterpriseList == null || enterpriseList.size() == 0) {
			System.out.println("========== enterpriseList is null  =============");
			return;
		}
		for (String enterpriseSourceKey : enterpriseList) {
			System.out.println("searching for enterpriseSourceKey: " + enterpriseSourceKey);

			Pageable sitePageRequest = new PageRequest(0, 1000000);
			Page<MetaEntityDTO> siteList = enterpriseService.getEnterpriseSites(enterpriseSourceKey, true,
					sitePageRequest);

			if (siteList == null || siteList.getSize() == 0) {
				System.out.println("========== sizelist is null or 0 size for enterpriseSourceKey ============="
						+ enterpriseSourceKey);
				continue;
			}

			for (MetaEntityDTO site : siteList) {
				String siteSourceKey = site.getSourceKey();
				Map<String, PointObject> mySiteHaystackMapping = new HashMap();
				HashMap<Integer, String> sitePeakHoursInfo = new HashMap<Integer, String>();
				HashMap<String, Integer[]> siteOccupancyInfo = new HashMap<String, Integer[]>();

				System.out.println(" !!!!!!!!! site MetaEntityDTO : " + site);

				String siteName = site.getName();
				if (siteName != null) {
					System.out.println(" !!!!!!!!! site Name: " + siteName);
					resourceNameMapping.put(siteSourceKey, siteName);
				}

				List<PropertiesDTO> siteProperties = site.getProperties();
				if (siteProperties != null && siteProperties.size() > 0) {
					for (PropertiesDTO prop : siteProperties) {
						if (prop.getId().equalsIgnoreCase("SqFt")) {
							Object[] value = prop.getValue();
							if (value != null) {
								String SqFt = (String) value[0];
								siteSqftMapping.put(siteSourceKey, Double.parseDouble(SqFt));
								System.out
										.println("========== siteSqftMapping populated for siteSourceKey ============="
												+ siteSourceKey);
							}

						}

						if (prop.getId().equalsIgnoreCase("OccupancySchedule")) {
							Object[] value = prop.getValue();
							if (value != null) {
								String occScheduleValue = (String) value[0];
								if (!StringUtils.isEmpty(occScheduleValue)) {
									JSONArray occHoursArr = new JSONArray(occScheduleValue);
									JSONObject obj = occHoursArr.getJSONObject(0);
									if (obj != null) {
										try {
											Integer[] weekdays = { Integer.parseInt(obj.getString("weekdayStartHour")),
													Integer.parseInt(obj.getString("weekdayEndHour")) };
											Integer[] weekends = { Integer.parseInt(obj.getString("weekendStartHour")),
													Integer.parseInt(obj.getString("weekendEndHour")) };

											Integer[] holidays = { Integer.parseInt(obj.getString("holidayStartHour")),
													Integer.parseInt(obj.getString("holidayEndHour")) };

											siteOccupancyInfo.put("weekdays", weekdays);
											siteOccupancyInfo.put("weekends", weekends);
											siteOccupancyInfo.put("holidays", holidays);
											logger.info("========== occupied hours for siteSourceKey ============="
													+ siteSourceKey + " are " + siteOccupancyInfo);
										} catch (Exception e) {
											System.out.println(
													"*** Holiday Hour Error : not configured for " + siteSourceKey);
											updateConfigErrorMap("INVALID_SITE_OCCUPANCY_SCHEDULE", siteSourceKey);
										}
									}
								}
							}
							siteOccupancyMapping.put(siteSourceKey, siteOccupancyInfo);
						}

						if (prop.getId().equalsIgnoreCase("EMSActivationDate")) {
							Object[] value = prop.getValue();
							if (value != null) {
								String EMSActivationDate = (String) value[0];
								// "01/24/2017" EMSActivationDate prop value as
								// of 01/24/2017
								String activationYear = EMSActivationDate.substring(EMSActivationDate.length() - 4,
										EMSActivationDate.length());
								siteInstallationYearsMapping.put(siteSourceKey, Integer.parseInt(activationYear));
								logger.info("========== activationYear populated for siteSourceKey ============="
										+ siteSourceKey);
							}
						}

						if (prop.getId().equalsIgnoreCase("peakSchedule")) {
							Object[] value = prop.getValue();
							if (value != null) {
								String peakScheduleValue = (String) value[0];
								logger.info("Peak Schedule" + peakScheduleValue);

								JSONArray peakHoursArr = null;
								try {
									peakHoursArr = new JSONArray(peakScheduleValue);
								} catch (Exception e) {
									System.out.println("Error in PeakHour Configuration: siteUid = " + siteSourceKey);
									updateConfigErrorMap("INVALID_SITE_PEAKHOUR_SCHEDULE", siteSourceKey);
									continue;
								}
								for (int i = 0; i < peakHoursArr.length(); i++) {
									JSONObject obj = peakHoursArr.getJSONObject(i);
									if (obj == null)
										continue;
									String label = obj.getString("label");
									System.out.println("************** Peak Hour   obj = ******************" + obj);
									if (label != null && obj.has("hours")) {
										System.out.println("************** Peak Hour   obj= ******************" + obj);
										String[] hours = obj.getString("hours").split("-");
										if (hours != null && hours.length > 0) {
											try {
												for (int j = Integer.parseInt(hours[0]); j < Integer
														.parseInt(hours[1]); j++) {
													sitePeakHoursInfo.put(j, label);
												}
											} catch (Exception ex) {
												System.err.print("Error parsing hours:" + hours);
											}
										}
									} else {
										System.out.println("************** No Hours   obj= ******************" + obj);
									}
								}

								sitePeakHoursMapping.put(siteSourceKey, sitePeakHoursInfo);
								logger.info("========== peakhours for siteSourceKey =============" + siteSourceKey
										+ " are " + sitePeakHoursInfo);
							}

						}
					}
				}

				System.out.println("========== locations for siteSourceKey =============" + siteSourceKey);

				List<LocationDTO> locations = siteService.getSiteLocations(siteSourceKey);
				String siteTimeZone = null;
				if (locations != null && locations.size() > 0) {
					LocationDTO loc = locations.get(0);
					GeoCoordinatesDTO geoCoordinate = loc.getGeoCoordinates();
					if (geoCoordinate != null) {
						siteTimeZone = geoCoordinate.getTimezone();
					}
				}

				siteTimeZoneMapping.put(siteSourceKey, siteTimeZone);
				siteEnterpriseMapping.put(siteSourceKey, enterpriseSourceKey);
				System.out.println("siteSourceKey: " + siteSourceKey);

				Pageable segmentPageRequest = new PageRequest(0, 1000000);
				MetaEntityDTO siteDescendantObj = siteService.getSiteDescendants(siteSourceKey, 1000, null,
						segmentPageRequest);

				if (siteDescendantObj == null) {
					System.out.println("====== siteDescendantObj is null for ========" + siteSourceKey);
					continue;
				}

				List<MetaEntityDTO> segmentList = siteDescendantObj.getChildren();
				if (segmentList == null || segmentList.size() == 0) {
					System.out
							.println("========== segmentList is null for siteSourceKey =============" + siteSourceKey);
					continue;
				}

				for (MetaEntityDTO segment : segmentList) {
					String segmentSourceKey = segment.getSourceKey();
					segmentSiteMapping.put(segmentSourceKey, siteSourceKey);
					segmentEnterpriseMapping.put(segmentSourceKey, enterpriseSourceKey);

					List<MetaEntityDTO> assetList = segment.getChildren();
					if (assetList == null || assetList.size() == 0) {
						System.out.println("====assetList is null or 0 size for segmentSourceKey ============="
								+ segmentSourceKey);
						continue;
					}

					for (MetaEntityDTO asset : assetList) {
						String assetSourceKey = asset.getSourceKey();

						String assetName = asset.getName();
						if (assetName != null) {
							System.out.println("*** AssetName = " + assetName);
							resourceNameMapping.put(assetSourceKey, assetName);
						}

						MetaTypeDTO metaType = asset.getMetaType();
						if (metaType == null)
							continue;

						String assetType = metaType.getName();
						assetEnterpriseMapping.put(assetSourceKey, enterpriseSourceKey);
						assetSiteMapping.put(assetSourceKey, siteSourceKey);
						assetTypeMapping.put(assetSourceKey, assetType);
						assetTimeZoneMapping.put(assetSourceKey, siteTimeZone);

						// store the gateway to site mapping for the JACE
						// edgeDeviceId for lookup.
						// the gateway will on the SITE level.
						if (assetType.equalsIgnoreCase("GATEWAY")) {
							String extRefId = asset.getExternalRefId();
							gatewaySiteMapping.put(extRefId, siteSourceKey);
						}

						// For the segment, it may have asset-segment in
						// one-to-many relationship.
						Set<String> assetSegmentList = assetSegmentMapping.get(assetSourceKey);
						if (assetSegmentList != null) {
							assetSegmentList.add(segmentSourceKey);
						} else {
							assetSegmentList = new HashSet<String>();
							assetSegmentList.add(segmentSourceKey);
							assetSegmentMapping.put(assetSourceKey, assetSegmentList);
						}
						System.out.println("==== Using ASSET SERVICE ====");
						MetaEntityDTO assetDTO = assetService.getAsset(assetSourceKey, false);

						List<PropertiesDTO> props = assetDTO.getProperties();
						if (props == null || props.size() == 0) {
							System.out.println("======props is null or 0 size  for assetSourceKey :" + assetSourceKey);
							continue;
						}

						for (PropertiesDTO prop : props) {
							if (prop.getId().equalsIgnoreCase("haystackMapping")) {
								System.out.println("**** COME TO HAYSTACK MAPPING ****");
								Object[] value = prop.getValue();
								if (value != null) {
									String strValue = (String) value[0];
									JSONArray arr = null;
									try {
										arr = new JSONArray(strValue);
									} catch (JSONException e) {
										System.out.println("*** Bad Haystack Mapping for asset: " + assetSourceKey);
										updateConfigErrorMap("INVALID_ASSET_HAYSTACK_MAPPING", assetSourceKey);
										break;
									}

									System.out.println(arr);

									Map<String, String> mapping = new HashMap<String, String>();
									for (int i = 0; i < arr.length(); i++) {
										JSONObject obj = arr.getJSONObject(i);
										try {
											String pointId = obj.getString("pointId");
											String pointName = obj.getString("pointName");
											if (pointId != null && pointName != null) {
												mapping.put(pointId, pointName);
												PointObject po = new PointObject(assetSourceKey, pointName);
												mySiteHaystackMapping.put(pointId, po);
											}
										} catch (JSONException e) {

											System.out.println("No PointID Found");
											continue;
										}
									}
									System.out.println(">>> Haystack Mapping, assetID = " + assetSourceKey
											+ "  mapping = " + mapping);
									haystackMapping.put(assetSourceKey, mapping);
								}

							}
						}

					}
				}
				siteHaystackMapping.put(siteSourceKey, mySiteHaystackMapping);
			}
			System.out.println("*** End Searching for enterprise: " + enterpriseSourceKey);

		}

		System.out.println("~~~~~~~~~~~~~~~~~~~~~assetEnterpriseMapping: " + assetEnterpriseMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~assetSiteMapping: " + assetSiteMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~assetSegmentMapping: " + assetSegmentMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~haystackMapping: " + haystackMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~assetTypeMapping: " + assetTypeMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~segmentSiteMapping: " + segmentSiteMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~segmentEnterpriseMapping: " + segmentEnterpriseMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~siteEnterpriseMapping: " + siteEnterpriseMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~assetTimeZoneMapping: " + assetTimeZoneMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~siteTimeZoneMapping: " + siteTimeZoneMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~gatewaySiteMapping: " + gatewaySiteMapping);

		System.out.println("~~~~~~~~~~~~~~~~~~~~~siteHaystackMapping: " + siteHaystackMapping);
		System.out.println("~~~~~~~~~~~~~~~~~~~~~billingList : " + billingList);
		for (String siteUid : siteHaystackMapping.keySet()) {
			System.out.println("== key = " + siteUid);
			Map<String, PointObject> pointMapping = siteHaystackMapping.get(siteUid);
			for (String pointName : pointMapping.keySet()) {
				System.out
						.println("== name = " + pointName + " pointObject = " + pointMapping.get(pointName).toString());
			}
		}
	}

	public void buildLoadTypeMapping(List<String> enterpriseUidList) {
		System.out.println("****** Build LoadType MAPPING *******");
		System.out.println("LoadType MAPPING enterprises: " + enterpriseUidList);

		Pageable pageRequest = new PageRequest(0, 100);
		List<String> enterpriseList = new ArrayList<String>();
		enterpriseList.addAll(enterpriseUidList);

		for (String enterpriseSourceKey : enterpriseList) {
			Pageable sitePageRequest = new PageRequest(0, 1000000);

			List<MetaTypeDTO> segmentList = enterpriseService.getTypesWithInstances(enterpriseSourceKey, "SEGMENT_TYPE",
					false);

			String loadTypeSourceKey = null;
			if (segmentList == null)
				return;

			for (MetaTypeDTO child : segmentList) {
				String name = child.getName();

				// get the load type ID.
				if (name.equalsIgnoreCase("LOAD TYPE")) {
					loadTypeSourceKey = child.getSourceKey();
					System.out.println("=== LoadType Uid = " + loadTypeSourceKey);
				}
			}
			if (loadTypeSourceKey == null)
				continue;

			List<MetaEntityDTO> childList = enterpriseService.getEnterpriseSegments(enterpriseSourceKey,
					loadTypeSourceKey, false);
			if (childList != null) {
				for (MetaEntityDTO child : childList) {
					String segmentUid = child.getSourceKey();
					System.out.println("=== Segment Uid = " + segmentUid);
					// let's add these loadtype segments to the billing list.
					billingList.add(segmentUid);

					MetaEntityDTO segmentDTO = segmentService.getSegment(segmentUid, false);

					// adding both loadType uid and name into the mapping
					String loadName = segmentDTO.getName();
					loadTypeMapping.put(segmentUid, loadTypeSourceKey + ":" + loadName);
					loadTypeSegmentNameMapping.put(segmentUid, child.getName());

					// CorrelatedEntity for the load type Segment to get
					// submeter list.
					segmentDTO.getProperties().forEach(sp -> {
						if (sp.getId().equalsIgnoreCase("correlatedEntity")) {
							Object[] submeterList = sp.getValue();
							Set<String> submeterUidList = new HashSet();

							// add the load type submeter into submeter Map.
							for (Object submeterObj : submeterList) {
								String submeterUid = (String) submeterObj;
								System.out.println("***** SegmentUid = " + segmentUid + " Submeter = " + submeterUid);
								submeterUidList.add(submeterUid);
							}
							segmentToSubmeterMapping.put(segmentUid, submeterUidList);
						}
					});
				}
			}
		}

		if (segmentToSubmeterMapping.size() > 0) {
			for (String segmentSourceKey : segmentToSubmeterMapping.keySet()) {
				Set<String> submeterList = segmentToSubmeterMapping.get(segmentSourceKey);
				// adding the submeter - segment information into
				// assetSegmentMapping.
				for (String submeterUid : submeterList) {
					Set<String> assetSegmentList = assetSegmentMapping.get(submeterUid);
					if (assetSegmentList != null) {
						assetSegmentList.add(segmentSourceKey);
					} else {
						assetSegmentList = new HashSet();
						assetSegmentList.add(segmentSourceKey);
						assetSegmentMapping.put(submeterUid, assetSegmentList);
					}
				}
			}
			System.out.println("LOADTYPE SUBMETER SEGMENT MAPPING >>>>" + assetSegmentMapping);
		}

		System.out.println("== Load Type Mapping size : " + loadTypeMapping.size());
	}

	public void buildSubmeterMapping(List<String> enterpriseUidList) {

		System.out.println("****** Build Submeter MAPPING *******");

		Pageable pageRequest = new PageRequest(0, 100);
		// Page<MetaEntityDTO> enterpriseList =
		// enterpriseService.getRootEnterprises(false, pageRequest);
		List<String> enterpriseList = new ArrayList<String>();
		enterpriseList.addAll(enterpriseUidList);

		if (enterpriseList == null || enterpriseList.size() == 0) {
			System.out.println("========== enterpriseList is null or 0 size =============");
			return;
		}

		Set<String> enterpriseSourceKeyList = new HashSet<>();
		for (String enterpriseSourceKey : enterpriseList) {
			Pageable sitePageRequest = new PageRequest(0, 1000000);
			// String enterpriseSourceKey = enterprise.getSourceKey();

			enterpriseSourceKeyList.add(enterpriseSourceKey);
			Page<MetaEntityDTO> childrenList = null;
			try {
				childrenList = enterpriseService.getEnterpriseChildren(enterpriseSourceKey, "ENTERPRISE", false,
						sitePageRequest);
			} catch (Exception e) {
				System.out.println("NO such enterprise found in APM remove it: " + enterpriseSourceKey);
				enterpriseSourceKeyList.remove(enterpriseSourceKey);
				updateConfigErrorMap("INVALID_ENTERPRISE_SOURCEKEY", enterpriseSourceKey);
				continue;
			}

			List<String> childrenSourceKeyList = new ArrayList<>();
			if (childrenList == null)
				continue;

			for (MetaEntityDTO child : childrenList) {
				String sourceKey = child.getSourceKey();
				System.out.println("==== sourceKey = " + sourceKey);
				childrenSourceKeyList.add(sourceKey);
			}
			enterpriseSourceKeyList.addAll(childrenSourceKeyList);
		}

		for (String enterpriseSourceKey : enterpriseSourceKeyList) {
			System.out.println("searching for enterpriseSourceKey: " + enterpriseSourceKey);

			Pageable sitePageRequest = new PageRequest(0, 1000000);
			Page<MetaEntityDTO> siteList = enterpriseService.getEnterpriseSites(enterpriseSourceKey, true,
					sitePageRequest);
			Set<String> enterpriseSubmeterList = new HashSet();

			// if no sites, we can just skip the submeter part.
			if (siteList == null || siteList.getSize() == 0) {
				continue;
			}

			for (MetaEntityDTO site : siteList) {
				String siteSourceKey = site.getSourceKey();
				System.out.println("**** siteSourceKey = " + siteSourceKey);
				List<PropertiesDTO> siteProps = site.getProperties();
				if (siteProps == null) {
					System.out.println("**** properties is NULL for  " + siteSourceKey);
					continue;
				}
				Set<String> siteSubmeterList = new HashSet();
				billingList.add(siteSourceKey);

				siteProps.forEach(p -> {
					// CostMeter for the Site
					System.out.println("*** properties = " + p.getId());
					if (p.getId().equalsIgnoreCase("correlatedEntity")) {
						Object[] costMeterList = p.getValue();
						for (Object costMeterObj : costMeterList) {
							String costMeterUid = (String) costMeterObj;
							System.out.println("***** CostMeter = " + costMeterUid);
							billingList.add(costMeterUid);

							Set<String> costMeterSubmeterList = new HashSet();
							MetaEntityDTO costMeterDTO = null;
							try {
								costMeterDTO = segmentService.getSegment(costMeterUid, false);
							} catch (Exception e) {
								System.out.println("*** Get Segment CostMeter Error, siteUid = " + siteSourceKey
										+ " , costMeterUid = " + costMeterUid);
								updateConfigErrorMap("INVALID_SITE_COSTMETER", siteSourceKey + ":" + costMeterUid);
								break;
							}
							if (costMeterDTO == null)
								continue;
							costMeterDTO.getProperties().forEach(cp -> {
								// CorrelatedEntity for the CostMeter.
								if (cp.getId().equalsIgnoreCase("correlatedEntity")) {
									Object[] segmentsList = cp.getValue();

									System.out.println("*** segmentslist size = " + segmentsList.length);

									for (Object segment : segmentsList) {
										String segmentUid = (String) segment;
										System.out.println("***** Segment = " + segmentUid);
										// now find out the correlated submeters
										// for these segments
										MetaEntityDTO segmentDTO = null;
										try {
											segmentDTO = segmentService.getSegment(segmentUid, false);
										} catch (Exception e) {
											// if it's not a segment, let's
											// assume it's submeter asset.
											siteSubmeterList.add(segmentUid);
											System.out.println("*** costMeter is configured to be ASSET, siteUid = "
													+ siteSourceKey + "  costMeter Uid = " + segmentUid);
											updateConfigErrorMap("INVALID_CORRELATED_ENTITY_FOR_COSTMETER",
													costMeterUid + ":" + segmentUid);
										}

										if (segmentDTO == null)
											continue;
										// CorrelatedEntity for the Segment to
										// get submeter list.
										segmentDTO.getProperties().forEach(sp -> {
											if (sp.getId().equalsIgnoreCase("correlatedEntity")) {
												Object[] submeterList = sp.getValue();
												Set<String> submeterUidList = new HashSet();

												for (Object submeterObj : submeterList) {
													String submeterUid = (String) submeterObj;
													System.out.println("***** Submeter = " + submeterUid);
													submeterUidList.add(submeterUid);
												}
												segmentToSubmeterMapping.put(segmentUid, submeterUidList);
												costMeterSubmeterList.addAll(submeterUidList);
												siteSubmeterList.addAll(submeterUidList);
												enterpriseSubmeterList.addAll(submeterUidList);
											}
										});
									}

									// now add all the correlated submeters to
									// the costMeter
									if (costMeterSubmeterList.size() > 0) {
										segmentToSubmeterMapping.put(costMeterUid, costMeterSubmeterList);
									}
								}
							});
						}
					}
				});

				if (siteSubmeterList.size() > 0) {
					siteToSubmeterMapping.put(siteSourceKey, siteSubmeterList);
				}
			}
			if (enterpriseSubmeterList.size() > 0) {
				enterpriseToSubmeterMapping.put(enterpriseSourceKey, enterpriseSubmeterList);

			}
		}

		System.out.println("SEGMENT SUBMETER MAPPING >>>> " + segmentToSubmeterMapping);
		System.out.println("SITE SUBMETER MAPPING >>>> " + siteToSubmeterMapping);
		System.out.println("ENTERPRISE SUBMETER MAPPING >>>> " + enterpriseToSubmeterMapping);

		System.out.println("==== Build Submeter to Segment Mapping ====");
		if (segmentToSubmeterMapping.size() > 0) {
			for (String segmentSourceKey : segmentToSubmeterMapping.keySet()) {
				Set<String> submeterList = segmentToSubmeterMapping.get(segmentSourceKey);
				// adding the submeter - segment information into
				// assetSegmentMapping.
				for (String submeterUid : submeterList) {
					Set<String> assetSegmentList = assetSegmentMapping.get(submeterUid);
					if (assetSegmentList != null) {
						assetSegmentList.add(segmentSourceKey);
					} else {
						assetSegmentList = new HashSet();
						assetSegmentList.add(segmentSourceKey);
						assetSegmentMapping.put(submeterUid, assetSegmentList);
					}
				}
			}
			System.out.println("SUBMETER SEGMENT MAPPING >>>>" + assetSegmentMapping);
		}
	}

	private List<String> getAllEnterprises() {
		List<String> allEnterprises = new ArrayList();
		Pageable pageRequest = new PageRequest(0, 500);
		Page<MetaEntityDTO> enterpriseEntityList = enterpriseService.getRootEnterprises(false, pageRequest);

		if (enterpriseEntityList == null)
			return allEnterprises;

		for (MetaEntityDTO enterprise : enterpriseEntityList) {
			String enterpriseUid = enterprise.getSourceKey();
			allEnterprises.add(enterpriseUid);
		}
		for (String key : allEnterprises) {
			System.out.println("*** Enterprise Uid = " + key);
		}
		return allEnterprises;
	}

	private void updateConfigErrorMap(String errorMsg, String resourceUid) {
		if (configErrorMap.containsKey(errorMsg)) {
			String currentUids = configErrorMap.get(errorMsg);
			configErrorMap.put(errorMsg, currentUids + ", " + resourceUid);
		} else {
			configErrorMap.put(errorMsg, resourceUid);
		}
	}

}
