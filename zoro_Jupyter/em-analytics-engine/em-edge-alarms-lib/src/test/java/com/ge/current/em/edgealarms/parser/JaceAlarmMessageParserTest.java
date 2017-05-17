package com.ge.current.em.edgealarms.parser;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ge.current.em.analytics.dto.SiteOfflineReadingDTO;

@RunWith(MockitoJUnitRunner.class)
public class JaceAlarmMessageParserTest {

	private static final String CSM_ALARM_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final DateTimeFormatter DTF =DateTimeFormatter.ofPattern(JaceAlarmMessageParser.JACE_TIME_FORMAT);
	
	private Map<String, String> siteTimeZoneMap = new HashMap<>();
	private Map<String, String> gatewaySiteMap = new HashMap<>();
	private Map<String, String> resourceNameMap = new HashMap<>();
	private Map<String, String> siteEnterpriseMap = new HashMap<>();

	@Mock
	Broadcast<Map<String, String>> gatewaySiteMapping;

	@Mock
	Broadcast<Map<String, String>> siteTimezoneMapping;

	@Mock
	Broadcast<Map<String, String>> resourceNameMapping;

	@Mock
	Broadcast<Map<String, String>> siteEnterpriseMapping;

	private JaceAlarmMessageParser parser;
	
	final ObjectMapper mapper = new ObjectMapper();

	@Before
	public void setup() {
		siteTimeZoneMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7", "Australia/Melbourne");
		gatewaySiteMap.put("986f288f-4819-4fbd-b953-5840687325c4", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("dbf82336-2294-4de7-a80c-fa32ccf71d37", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("2676a2a9-60e6-4926-9259-cca6a6b67405", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("0b1a9380-4e76-4f1f-89a2-7bf553555ea0", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("WX-123", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		resourceNameMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7", "Daintree Melbourne");
		siteEnterpriseMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7",
				"ENTERPRISE_3fe239c9-3531-478c-ada1-ec66f82516a7");

		parser = new JaceAlarmMessageParser();
	}
	
	private byte[] loadData(final String path) {
		byte[] encoded;
		try {
			encoded = Files.readAllBytes(Paths.get("src/test/resources/" + path));
			return encoded;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	
	@Test
	public void test() throws JsonProcessingException {
		final EdgeAlarmDTO alarm = new EdgeAlarmDTO();
		alarm.setAction("RAISED");
		alarm.setEdgeDeviceID("WX-123");
		alarm.setTime("2017-04-19T15:33:32.05-07:00[America/Los_Angeles]");
		alarm.setAlarm(0);
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(alarm));
	}
	
	@Test
	public void testFormatter() {
		ZonedDateTime readingZonedDateTime = ZonedDateTime.now();
		System.out.println(DTF.format(readingZonedDateTime));
	}
	
	@Test
	public void testParse() {
		when(siteTimezoneMapping.value()).thenReturn(siteTimeZoneMap);
		when(gatewaySiteMapping.value()).thenReturn(gatewaySiteMap);
		when(resourceNameMapping.value()).thenReturn(resourceNameMap);
		when(siteEnterpriseMapping.value()).thenReturn(siteEnterpriseMap);
		
		try {
			final List<SiteOfflineReadingDTO> readings = parser.getSiteReadingDTO(loadData("jace-alarm.json"), gatewaySiteMapping, siteTimezoneMapping);
			for (SiteOfflineReadingDTO dto : readings) {
				System.out.println(ReflectionToStringBuilder.toString(dto, ToStringStyle.SIMPLE_STYLE));
			}
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}


}
