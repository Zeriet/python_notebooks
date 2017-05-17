//package com.ge.current.em.aggregation.parser;
//
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//
//import com.ge.current.em.analytics.dto.PointDTO;
//import com.ge.current.em.analytics.dto.SiteReadingDTO;
//import com.ge.current.em.analytics.dto.daintree.attribute.Attribute;
//import com.ge.current.em.analytics.dto.daintree.dto.EventEntry;
//import com.ge.current.em.analytics.dto.daintree.dto.TimeSeriesLogDTO;
//
//import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
//import org.apache.hadoop.test.MockitoUtil;
//import org.apache.spark.broadcast.Broadcast;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.internal.util.MockUtil;
//import org.mockito.runners.MockitoJUnitRunner;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.mockito.Mockito.*;
//
//@RunWith(MockitoJUnitRunner.class)
//public class WacMessageParserTest {
//
//	private Map<String, String> siteTimeZoneMap = new HashMap<>();
//	private Map<String, String> gatewaySiteMap = new HashMap<>();
//
//	@Mock
//	Broadcast<Map<String, String>> gatewaySiteMapping;
//
//	@Mock
//	Broadcast<Map<String, String>> siteTimezoneMapping;
//
//	@Before
//	public void setup() {
//		siteTimeZoneMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7", "Australia/Melbourne");
//		gatewaySiteMap.put("3fe239c9-3531-478c-ada1-ec66f82516a7", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
//	}
//
//	//@Test
//	public void testRTUPoints() {
//		final WacMessageParser parser = new WacMessageParser();
//
//		// Setup the mock data
//		when(siteTimezoneMapping.value()).thenReturn(siteTimeZoneMap);
//		when(gatewaySiteMapping.value()).thenReturn(gatewaySiteMap);
//		byte[] encoded;
//		try {
//			encoded = Files.readAllBytes(Paths.get("src/test/resources/csm-rtu.json"));
//			List<SiteReadingDTO> results = parser.getSiteReadingDTO(encoded, gatewaySiteMapping, siteTimezoneMapping);
//			assertEquals(1, results.size());
//
//			// Verify each reading
//			for (SiteReadingDTO dto : results) {
//				System.out.println(ReflectionToStringBuilder.toString(dto));
//				for (PointDTO point : dto.getPoints()) {
//					if (point.getPointName().endsWith(Attribute.ENERGY_USAGE.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("0.22", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.ENERGY_USAGE_KW.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("0.22", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.THERMOSTAT_COOLING_MODE.name())) {
//						assertEquals("Boolean", point.getPointType());
//						assertTrue(Boolean.parseBoolean(point.getCurValue().toString()));
//					} else if (point.getPointName().endsWith(Attribute.THERMOSTAT_SYSTEM_MODE.name())) {
//						assertEquals("Enum", point.getPointType());
//						assertEquals("COOLING", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.THERMOSTAT_MANUAL_OVERRIDE.name())) {
//						assertEquals("Boolean", point.getPointType());
//						assertTrue(Boolean.parseBoolean(point.getCurValue().toString()));
//					} else if (point.getPointName().endsWith(Attribute.THERMOSTAT_RUNNING_STATE.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("2", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.THERMOSTAT_COOLING_1_STATE.name())) {
//						assertEquals("Boolean", point.getPointType());
//						assertTrue(Boolean.parseBoolean(point.getCurValue().toString()));
//					} else if (point.getPointName().endsWith(Attribute.THERMOSTAT_EMERGENCY_HEATING_STATE.name())) {
//						assertEquals("Boolean", point.getPointType());
//						assertFalse(Boolean.parseBoolean(point.getCurValue().toString()));
//					}  else if (point.getPointName().endsWith(Attribute.THERMOSTAT_FAN_1_STATE.name())) {
//						assertEquals("Boolean", point.getPointType());
//						assertTrue(Boolean.parseBoolean(point.getCurValue().toString()));
//					} else if (point.getPointName().endsWith(Attribute.TEMPERATURE_SETPOINT_COOLING.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("68.0", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.TEMPERATURE_THERMOSTAT.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("74.66", point.getCurValue());
//					}
//				}
//			}
//
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			fail(ex.getMessage());
//		}
//	}
//
//	//@Test
//	public void testLightingPoints() {
//		final WacMessageParser parser = new WacMessageParser();
//
//		// Setup the mock data
//		when(siteTimezoneMapping.value()).thenReturn(siteTimeZoneMap);
//		when(gatewaySiteMapping.value()).thenReturn(gatewaySiteMap);
//		byte[] encoded;
//		try {
//			encoded = Files.readAllBytes(Paths.get("src/test/resources/csm-lights.json"));
//			List<SiteReadingDTO> results = parser.getSiteReadingDTO(encoded, gatewaySiteMapping, siteTimezoneMapping);
//			assertEquals(2, results.size());
//
//			// Verify each reading
//			for (SiteReadingDTO dto : results) {
//				System.out.println(ReflectionToStringBuilder.toString(dto));
//				for (PointDTO point : dto.getPoints()) {
//					if (point.getPointName().endsWith(Attribute.LIGHT_INTENSITY.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("90", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.LIGHT_STATE.name())) {
//						assertEquals("Boolean", point.getPointType());
//						validateBoolean(point);
//					} else if (point.getPointName().endsWith(Attribute.ENERGY_USAGE.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("0.02", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.ENERGY_USAGE_KW.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("0.02", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.ENERGY_USAGE_KW.name())) {
//						assertEquals("Numeric", point.getPointType());
//						assertEquals("0.02", point.getCurValue());
//					} else if (point.getPointName().endsWith(Attribute.OCCUPANCY.name())) {
//						assertEquals("Boolean", point.getPointType());
//						validateBoolean(point);
//					}
//				}
//			}
//
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			fail(ex.getMessage());
//		}
//
//	}
//
//	private void validateBoolean(final PointDTO dto) {
//		assertNotNull(Boolean.parseBoolean(dto.getCurValue().toString()));
//	}
//
//	@Test
//	public void testParserValid() {
//		final WacMessageParser parser = new WacMessageParser();
//		byte[] encoded;
//		try {
//			encoded = Files.readAllBytes(Paths.get("src/test/resources/csm-ts.json"));
//			final List<TimeSeriesLogDTO> dto = parser.mapPayloadToLogs(encoded);
//			for (TimeSeriesLogDTO obj : dto) {
//				System.out.println(org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(obj));
//			}
//			assertEquals(4, dto.size());
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			fail(ex.getMessage());
//		}
//
//	}
//
//	@Test
//	public void testParserError() {
//		final WacMessageParser parser = new WacMessageParser();
//		byte[] encoded;
//		try {
//			encoded = Files.readAllBytes(Paths.get("src/test/resources/csm-ts-old.json"));
//			final List<TimeSeriesLogDTO> dto = parser.mapPayloadToLogs(encoded);
//			for (TimeSeriesLogDTO obj : dto) {
//				System.out.println(org.apache.commons.lang3.builder.ReflectionToStringBuilder.toString(obj));
//			}
//			assertEquals(0, dto.size());
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			fail(ex.getMessage());
//		}
//
//	}
//
//}
