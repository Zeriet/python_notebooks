package com.ge.current.em.util;

import com.ge.current.em.entities.analytics.Event;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;
import com.ge.current.ie.constant.AppConstant;
import com.ge.current.ie.model.nosql.EventLog;
import com.ge.current.ie.util.EmailUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import java.util.TimeZone;

public class EmUtil {

	static final Logger LOG = LoggerFactory.getLogger(EmUtil.class);

	public static SQLContext initSqlContextAndCache(SparkContext context, PropertiesBean properties) {

		SQLContext sqlContext = new SQLContext(context);

		return initCache(sqlContext, properties);
	}

	private static SQLContext initCache(SQLContext sqlContext, PropertiesBean properties) {
		Map<String, String> options = getCacheConnectionOptions(properties);

		String schema = properties.getValue("cache.schema");

		LOG.info("JDBC URL: {}, user: {}, schema: {}", options.get("url"), options.get("user"), schema);
		Map<String, String> schemaTableMap = EmUtil.getSchemaTables();
		Set<String> schemaTables = schemaTableMap.keySet();

		for (String tableName : schemaTables) {
			loadCacheForEntity(sqlContext, schema, tableName, options, schemaTableMap.get(tableName));
		}
		String[] strings = sqlContext.tableNames();
		//		LOG.info("strings length: " + strings.length);
		return sqlContext;
	}

	public static Map<String, String> getCacheConnectionOptions(PropertiesBean properties) {
		Map<String, String> options = new HashMap<String, String>();

		options.put("url", properties.getValue(AppConstant.CACHE_URL));
		options.put("driver", properties.getValue(AppConstant.CACHE_DRIVER));
		options.put("user", properties.getValue(AppConstant.CACHE_USER));
		options.put("password", properties.getValue(AppConstant.CACHE_PSWD));

		return options;
	}

	private static void loadCacheForEntity(SQLContext sqlContext, String schema, String entityName, Map<String, String> options, String tempTableName) {
		options.put("dbtable", schema + "." + entityName);
		DataFrame df = sqlContext.read().format("jdbc").options(options).load();
		df.registerTempTable(tempTableName);
	}

	/**
	 * @param sqlContext
	 * @param ruleUids   TODO
	 * @return A map where keys=rule_uid, values=document_raw
	 */
	public static Map<String, String> getAllDocumentForRuleId(SQLContext sqlContext, String ruleUids) {
		Map<String, String> rulesDrl = new HashMap<>();

		DataFrame result = sqlContext.sql("SELECT DISTINCT(rule.rule_name), document_log.document_raw FROM rule,document_log WHERE "
						+ "rule.document_uid = document_log.document_uid");
		List<Map<String, String>> drlMapList = result.javaRDD().map(row -> {
			Map<String, String> currentRuleDrl = new HashMap<>();
			currentRuleDrl.put(row.getAs("rule_name"), new String((byte[]) row.getAs("document_raw")));
			return currentRuleDrl;
		}).collect();

		drlMapList.forEach(e -> rulesDrl.putAll(e));
		rulesDrl.entrySet().forEach(entry -> {
			//		    LOG.info(" drlMapList to broadcast Key : {}, Value : {}",entry.getKey(), entry.getValue());
		});
		return rulesDrl;
	}

	/**
	 * @param sqlContext
	 * @return a map where keys=rule_uid, values=rule_name
	 */
	public static Map<String, String> getRuleNamesMapping(SQLContext sqlContext) {
		Map<String, String> ruleNameMap = new HashMap<>();
		DataFrame result = sqlContext.sql("SELECT rule.rule_uid, rule.rule_name FROM rule");
		result.show();
		List<Map<String, String>> ruleNameMapAsList = result.javaRDD().map(row -> {
			Map<String, String> currentRuleNameMap = new HashMap<>();
			currentRuleNameMap.put(row.getAs("rule_uid"), row.getAs("rule_name"));
			return currentRuleNameMap;
		}).collect();
		ruleNameMapAsList.forEach(map -> ruleNameMap.putAll(map));
		ruleNameMap.entrySet().forEach(entry -> {
			LOG.warn(" ruleNameMap to broadcast Key : {}, Value : {}", entry.getKey(), entry.getValue());
		});
		return ruleNameMap;
	}

	public static Map<String, Map<String, Object>> getRulePropertiesMap(SQLContext sqlContext, String ruleUids) {
		Map<String, Map<String, Object>> ruleProperties = new HashMap<>();

		DataFrame result = sqlContext.sql("SELECT  rule.rule_id, rule_property.name, rule_property.value FROM rule, rule_property WHERE "
						+ "rule_property.rule_property_type='IN-PARAMETER' and rule_property.rule_id=rule.rule_id and rule_property.name not in ('REQUIRED_TAGS', 'REQUIRED_MEASURES', 'REQUIRED_MEASURES_AVG', 'REQUIRED_MEASURES_AGGR', 'ASSET_TYPE', 'time_of_day')");

		Map<String, Map<String, Object>> currentProperty = new HashMap<>();
		// To Do refactor this
		List<Map<String, Map<String, Object>>> propertiesAsList = result.javaRDD().map(row -> {
			Map<String, Object> properties = null;
			if (currentProperty.get(String.valueOf(row.getInt(0))) == null) {
				properties = new HashMap<>();
			} else {
				properties = currentProperty.get(String.valueOf(row.getInt(0)));
			}
			properties.put(row.getAs("name"), row.getAs("value"));
			currentProperty.put(String.valueOf(row.getInt(0)), properties);
			return currentProperty;

		}).collect();
		propertiesAsList.forEach(e -> ruleProperties.putAll(e));
		ruleProperties.entrySet().forEach(entry -> {
			LOG.warn(" rulePropertiesMap to broadcast Key : {}, Value : {}", entry.getKey(), entry.getValue());
		});
		return ruleProperties;
	}

	public static Map<String, List<String>> getRuleMeasuresMap(SQLContext sqlContext, String ruleUids) {
		Map<String, List<String>> ruleMeasuresMapToBroadCast = new HashMap<>();

		DataFrame result = sqlContext.sql("SELECT  rule.rule_name, rule_property.value FROM rule, rule_property WHERE "
				+ "rule_property.rule_property_type='IN-PARAMETER' and rule_property.rule_id=rule.rule_id and rule_property.name='REQUIRED_MEASURES' group by rule.rule_name, rule_property.value");

		Map<String, List<String>> currentProperty = new HashMap<>();
		// To Do refactor this
		List<Map<String, List<String>>> propertiesAsList = result.javaRDD().map(row -> {
			List<String> properties = null;
			if (currentProperty.get(row.getAs("rule_name")) == null) {
				properties = new ArrayList<>();
			} else {
				properties = currentProperty.get(row.getAs("rule_name"));
			}
			properties.add(row.getString(1));
			currentProperty.put(row.getAs("rule_name"), properties);
			return currentProperty;

		}).collect();
		for (Map<String, List<String>> map : propertiesAsList) {
			for (String key : map.keySet()) {
				List<String> properties = null;
				if (ruleMeasuresMapToBroadCast.get(key) == null) {
					properties = new ArrayList<>();
				} else {
					properties = ruleMeasuresMapToBroadCast.get(key);
				}
				properties.addAll(map.get(key));
				ruleMeasuresMapToBroadCast.put(key, properties);
			}
		}
		ruleMeasuresMapToBroadCast.entrySet().forEach(entry -> {
			LOG.warn(" getRuleMeasuresMap to broadcast Key : {}, Value : {}", entry.getKey(), entry.getValue());
		});
		return ruleMeasuresMapToBroadCast;
	}

	public static Map<String, List<String>> getRuleMeasuresAggrMap(SQLContext sqlContext, String ruleUids) {
		Map<String, List<String>> ruleMeasuresMapToBroadCast = new HashMap<>();

		DataFrame result = sqlContext.sql("SELECT  rule.rule_name, rule_property.value FROM rule, rule_property WHERE "
				+ "rule_property.rule_property_type='IN-PARAMETER' and rule_property.rule_id=rule.rule_id and rule_property.name='REQUIRED_MEASURES_AGGR' group by rule.rule_name, rule_property.value");

		Map<String, List<String>> currentProperty = new HashMap<>();
		// To Do refactor this
		List<Map<String, List<String>>> propertiesAsList = result.javaRDD().map(row -> {
			List<String> properties = null;
			if (currentProperty.get(row.getAs("rule_name")) == null) {
				properties = new ArrayList<>();
			} else {
				properties = currentProperty.get(row.getAs("rule_name"));
			}
			properties.add(row.getString(1));
			currentProperty.put(row.getAs("rule_name"), properties);
			return currentProperty;

		}).collect();
		for (Map<String, List<String>> map : propertiesAsList) {
			for (String key : map.keySet()) {
				List<String> properties = null;
				if (ruleMeasuresMapToBroadCast.get(key) == null) {
					properties = new ArrayList<>();
				} else {
					properties = ruleMeasuresMapToBroadCast.get(key);
				}
				properties.addAll(map.get(key));
				ruleMeasuresMapToBroadCast.put(key, properties);
			}
		}
		ruleMeasuresMapToBroadCast.entrySet().forEach(entry -> {
			LOG.warn(" ruleMeasuresAggrMapToBroadCast to broadcast Key : {}, Value : {}", entry.getKey(), entry.getValue());
		});
		return ruleMeasuresMapToBroadCast;
	}

	public static Map<String, List<String>> getRuleMeasuresAvgMap(SQLContext sqlContext, String ruleUids) {
		Map<String, List<String>> ruleMeasuresMapToBroadCast = new HashMap<>();

		DataFrame result = sqlContext.sql("SELECT  rule.rule_name, rule_property.value FROM rule, rule_property WHERE "
				+ "rule_property.rule_property_type='IN-PARAMETER' and rule_property.rule_id=rule.rule_id and rule_property.name='REQUIRED_MEASURES_AVG' group by rule.rule_name, rule_property.value");
		Map<String, List<String>> ruleMeasuresMap = new HashMap<>();
		// To Do refactor this
		List<Map<String, List<String>>> propertiesAsList = result.javaRDD().map(row -> {
			List<String> properties = null;
			if (ruleMeasuresMap.get(row.getAs("rule_name")) == null) {
				properties = new ArrayList<>();
			} else {
				properties = ruleMeasuresMap.get(row.getAs("rule_name"));
			}
			properties.add(row.getString(1));
			ruleMeasuresMap.put(row.getAs("rule_name"), properties);
			return ruleMeasuresMap;

		}).collect();
		for (Map<String, List<String>> map : propertiesAsList) {
			for (String key : map.keySet()) {
				List<String> properties = null;
				if (ruleMeasuresMapToBroadCast.get(key) == null) {
					properties = new ArrayList<>();
				} else {
					properties = ruleMeasuresMapToBroadCast.get(key);
				}
				properties.addAll(map.get(key));
				ruleMeasuresMapToBroadCast.put(key, properties);
			}
		}
		ruleMeasuresMapToBroadCast.entrySet().forEach(entry -> {
			LOG.warn(" getRuleMeasuresAvgMap to broadcast Key : {}, Value : {}", entry.getKey(), entry.getValue());
		});
		return ruleMeasuresMapToBroadCast;
	}

	public static Map<String, List<String>> getRuleTagsMap(SQLContext sqlContext, String ruleUids) {
		Map<String, List<String>> rulesRequiredTagsToBroadcast = new HashMap<>();

		final Map<String, List<String>> rulesRequiredTags = new HashMap<>();

		DataFrame result = sqlContext.sql("SELECT  rule.rule_name,rule_property.value FROM rule, rule_property WHERE " + "rule_property.rule_property_type='IN-PARAMETER' and rule_property.rule_id=rule.rule_id and rule_property.name='REQUIRED_TAGS' group by rule.rule_name, rule_property.value");

		// To Do refactor this
		List<Map<String, List<String>>> propertiesAsList = result.javaRDD().map(row -> {
			List<String> properties = null;
			if (rulesRequiredTags.get(row.getAs("rule_name")) == null) {
				properties = new ArrayList<>();
			} else {
				properties = rulesRequiredTags.get(row.getAs("rule_name"));
			}
			properties.add(row.getString(1));
			rulesRequiredTags.put(row.getAs("rule_name"), properties);
			return rulesRequiredTags;

		}).collect();
		for (Map<String, List<String>> map : propertiesAsList) {
			for (String key : map.keySet()) {
				List<String> properties = null;
				if (rulesRequiredTagsToBroadcast.get(key) == null) {
					properties = new ArrayList<>();
				} else {
					properties = rulesRequiredTagsToBroadcast.get(key);
				}
				properties.addAll(map.get(key));
				rulesRequiredTagsToBroadcast.put(key, properties);
			}
		}
		rulesRequiredTagsToBroadcast.entrySet().forEach(entry -> {
			LOG.warn(" rulesRequiredTagsToBroadcast to broadcast Key : {}, Value : {}", entry.getKey(), entry.getValue());
		});
		return rulesRequiredTagsToBroadcast;
	}
	/*
	 * rule_id - > Asset_type association to see the required assetTypes
	 *  
	 */

	/// rule association with asset_type, enterprise,segment,site. This can be used during filtering
	public static Map<String, List<String>> getRuleAssetTypeFilterMap(SQLContext sqlContext, String ruleUids) {
		Map<String, List<String>> rulesRequiredAssetTypesToBroadcast = new HashMap<>();
		Map<String, List<String>> rulesRequiredAssetTypes = new HashMap<>();
		DataFrame result = sqlContext.sql("SELECT  rule.rule_name, rule_property.value  FROM rule, rule_property WHERE "
				+ "rule_property.rule_property_type='IN-PARAMETER' and rule_property.rule_id=rule.rule_id and rule_property.name='ASSET_TYPE' group by rule.rule_name, rule_property.value");

		// To Do refactor this
		List<Map<String, List<String>>> propertiesAsList = result.javaRDD().map(row -> {
			String ruleName = row.getAs("rule_name");
			List<String> properties = null;
			LOG.info("rulesRequiredAssetTypes.get(ruleName): " + rulesRequiredAssetTypes.get(ruleName));
			if (rulesRequiredAssetTypes.get(ruleName) == null) {
				properties = new ArrayList<>();
			} else {
				properties = rulesRequiredAssetTypes.get(ruleName);
			}
			properties.add(row.getString(1));
			rulesRequiredAssetTypes.put(ruleName, properties);
			return rulesRequiredAssetTypes;

		}).collect();

		for (Map<String, List<String>> map : propertiesAsList) {
			for (String key : map.keySet()) {
				List<String> properties = null;
				if (rulesRequiredAssetTypesToBroadcast.get(key) == null) {
					properties = new ArrayList<>();
				} else {
					properties = rulesRequiredAssetTypesToBroadcast.get(key);
				}
				properties.addAll(map.get(key));
				rulesRequiredAssetTypesToBroadcast.put(key, properties);
			}
		}
		rulesRequiredAssetTypesToBroadcast.entrySet().forEach(
				e -> LOG.warn(" rsRequiredAssetTypes to broadcast Key : {}, Value : {}", e.getKey(), e.getValue()));

		return rulesRequiredAssetTypesToBroadcast;
	}

	/*
	 * This is basically to get Ruleid for the lookup for all required parameters, Assets and tags
	 */
	public static Map<String, String> getRuleRegistryMap(SQLContext sqlContext, String ruleUids) {
		Map<String, String> ruleProperties = new HashMap<>();

		DataFrame result = sqlContext.sql("SELECT rule.rule_name,rule.rule_id, rule_registry.entity_uid FROM rule, rule_registry WHERE "
						+ "rule_registry.rule_id=rule.rule_id and rule_registry.thru_date = '9999-12-31'");
		Map<String, String> properties = new HashMap<>();
		// To Do refactor this
		List<Map<String, String>> ruleRegistryMap = result.javaRDD().map(row -> {

			String key = row.getAs("rule_name") + "_" + row.getAs("entity_uid");

			properties.put(key, String.valueOf(row.getInt(1)));
			return properties;

		}).collect();
		ruleRegistryMap.forEach(e -> ruleProperties.putAll(e));
		//		ruleProperties.entrySet().forEach(entry -> {
		//		    LOG.info(" getRuleRegistryMap to broadcast Key : {}, Value : {}",entry.getKey(), entry.getValue());
		//		});
		return ruleProperties;
	}

	/**
	 * @param list          list to be sorted
	 * @param keyForSorting key used for sorting
	 * @return sorted list
	 */
	public static List<Map<String, Object>> sortListOfMapsBasedOnKey(List<Map<String, Object>> list,
			String keyForSorting) {
		list.sort(Comparator.comparing(map -> ((map.get(keyForSorting) != null) ? (String) map.get(keyForSorting) : null)));
		return list;
	}

	public static final Map<String, String> getSchemaTables() {
		HashMap schemaTableMap = new HashMap();
		schemaTableMap.put("document_log", "document_log");
		schemaTableMap.put("enum", "enum");
		schemaTableMap.put("rule", "rule");
		schemaTableMap.put("rule_property", "rule_property");
		schemaTableMap.put("rule_registry", "rule_registry");
		return schemaTableMap;
	}

	/**
	 * @param utcEpochTimeInString UTC time in string format
	 * @param timeZoneLocal        e.g. America/Los_Angeles
	 * @param timeFormat           e.g yyyyMMdd or HHmm
	 * @return local time in the format required
	 */
	public static String convertUTCStringTimeToLocalTime(String utcEpochTimeInString, String timeZoneLocal, String timeFormat) {
		ZoneId localZoneId = ZoneId.of(timeZoneLocal);
		Instant instant = new Date(Long.parseLong(utcEpochTimeInString)).toInstant();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
		return ZonedDateTime.ofInstant(instant, localZoneId).format(formatter);
	}

	/**
	 *
	 * @param eventTS epoch timestamp
	 * @param eventTsTz local timezone
	 * @return local time in string
	 * @throws ParseException
	 */
	public static String convertEventTsToLocalDateTime(String eventTS, String eventTsTz) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date date = formatter.parse(eventTS);
		String epochTime = String.valueOf(date.toInstant().toEpochMilli());
		String localDateTime = EmUtil.convertUTCStringTimeToLocalTime(epochTime, "America/Los_Angeles", "yyyyMMddHHmm");
		return localDateTime;
	}
}
