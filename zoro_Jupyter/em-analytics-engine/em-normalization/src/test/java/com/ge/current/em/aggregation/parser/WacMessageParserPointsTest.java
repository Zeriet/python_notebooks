package com.ge.current.em.aggregation.parser;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.ge.current.em.analytics.dto.PointDTO;
import com.ge.current.em.analytics.dto.SiteReadingDTO;
import com.ge.current.em.analytics.dto.daintree.attribute.Attribute;
import com.ge.current.em.analytics.dto.daintree.dto.EventEntry;
import com.ge.current.em.analytics.dto.daintree.dto.TimeSeriesLogDTO;
import com.ge.current.em.aggregation.request.SiteReadingDTOContainer;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.test.MockitoUtil;
import org.apache.spark.broadcast.Broadcast;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.MockUtil;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class WacMessageParserPointsTest {

	private Map<String, String> siteTimeZoneMap = new HashMap<>();
	private Map<String, String> gatewaySiteMap = new HashMap<>();

	@Mock
	Broadcast<Map<String, String>> gatewaySiteMapping;

	@Mock
	Broadcast<Map<String, String>> siteTimezoneMapping;

	@Before
	public void setup() {
		siteTimeZoneMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7", "Australia/Melbourne");
		gatewaySiteMap.put("5e5ab18e-4e04-4cdd-8f98-171b7699a38a", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
	}

	@Test
	public void testRTUPoints() {
		final WacMessageParser parser = new WacMessageParser();

		// Setup the mock data
		when(siteTimezoneMapping.value()).thenReturn(siteTimeZoneMap);
		when(gatewaySiteMapping.value()).thenReturn(gatewaySiteMap);
		byte[] encoded;
		try {
			encoded = Files.readAllBytes(Paths.get("src/test/resources/multiple-points.json"));
			List<SiteReadingDTOContainer> resultsList = parser.getSiteReadingDTO(encoded, gatewaySiteMapping, siteTimezoneMapping);
            List<SiteReadingDTO> results = new ArrayList();
            
            if (resultsList != null) {
                for(SiteReadingDTOContainer container: resultsList) {
                    results.add(container.getSiteReadingDTO());
                }
            }
			assertEquals(1, results.size());
			assertEquals(2, results.get(0).getPoints().size());
			
			
			// Verify each reading
			for (SiteReadingDTO dto : results) {
				System.out.println(dto.toString());
				for (PointDTO point : dto.getPoints()) {
					if (point.getPointName().endsWith("LIGHT_STATE")) {
						assertEquals(3, point.getPointReadings().size());
					} else {
						assertEquals(2, point.getPointReadings().size());
					}
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.getMessage());
		}
	}

}
