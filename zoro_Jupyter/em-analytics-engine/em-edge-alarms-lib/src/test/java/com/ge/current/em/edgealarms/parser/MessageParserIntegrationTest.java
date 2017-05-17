package com.ge.current.em.edgealarms.parser;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.spark.broadcast.Broadcast;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.daintreenetworks.haystack.commons.model.alarm.AlarmAction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ge.current.em.analytics.dto.daintree.dto.AlarmEntry;
import com.ge.current.em.analytics.dto.daintree.dto.AlarmMessageDTO;
import com.ge.current.em.entities.analytics.AlertLog;

@RunWith(MockitoJUnitRunner.class)
public class MessageParserIntegrationTest {
	
	private static final String CSM_ALARM_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final org.joda.time.format.DateTimeFormatter DTF = org.joda.time.format.DateTimeFormat
			.forPattern(CSM_ALARM_DATE_TIME_FORMAT).withZoneUTC();
	
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

	private WacAlarmMessageParser parser;
	
	final ObjectMapper mapper = new ObjectMapper();

	@Before
	public void setup() {
		siteTimeZoneMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7", "Australia/Melbourne");
		gatewaySiteMap.put("986f288f-4819-4fbd-b953-5840687325c4", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("dbf82336-2294-4de7-a80c-fa32ccf71d37", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("2676a2a9-60e6-4926-9259-cca6a6b67405", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("0b1a9380-4e76-4f1f-89a2-7bf553555ea0", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		gatewaySiteMap.put("c32239c6-655c-428a-9b15-00ec77ba9c94", "SITE_3fe239c9-3531-478c-ada1-ec66f82516a7");
		resourceNameMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7", "Daintree Melbourne");
		siteEnterpriseMap.put("SITE_3fe239c9-3531-478c-ada1-ec66f82516a7",
				"ENTERPRISE_3fe239c9-3531-478c-ada1-ec66f82516a7");

		parser = new WacAlarmMessageParser();
		DbHelper.cleanUp();

	}

	@BeforeClass
	public static void connectDb() throws IOException {
		try {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra("cu-cassandra.yaml");
		} catch (ConfigurationException | TTransportException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		DbHelper.prepareDb();
	}

	private void sendData(final AlarmEntry entry) throws Exception {
		when(siteTimezoneMapping.value()).thenReturn(siteTimeZoneMap);
		when(gatewaySiteMapping.value()).thenReturn(gatewaySiteMap);
		when(resourceNameMapping.value()).thenReturn(resourceNameMap);
		when(siteEnterpriseMapping.value()).thenReturn(siteEnterpriseMap);
		
		parser.processAlarmData(createMessage(entry), gatewaySiteMapping, siteTimezoneMapping,
				resourceNameMapping, siteEnterpriseMapping, DbHelper.getProperties());
		
	}
	
	private byte[] createMessage(final AlarmEntry entry) {
		final AlarmMessageDTO message = new AlarmMessageDTO();
		message.getData().add(entry);
		String jsonStr;
		try {
			jsonStr = mapper.writeValueAsString(message);
			System.out.println(jsonStr);
			return jsonStr.getBytes();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
	private AlarmEntry createEntry(final AlarmAction action, final String time) {
		return new AlarmEntry().setAction(action).setFacilityID("986f288f-4819-4fbd-b953-5840687325c4").setAlarmId(AlarmConstants.OFFLINE_ALARMS.get(0)).setTimeRef(time);
		
	}
	
	@Test
	public void testFirstAlert() throws Exception {
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:30:55"));
		// There should be one
		final List<AlertLog> results = DbHelper.getAllInAlertTable();
		assertEquals(1, results.size());
		assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, results.get(0).getStatus());
		DbHelper.printLogs(results);
	}
	
	@Test
	public void testSameAlert() throws Exception {
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:30:55"));
		
		// There should be one
		List<AlertLog> results = DbHelper.getAllInAlertTable();
		assertEquals(1, results.size());
		assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, results.get(0).getStatus());
		DbHelper.printLogs(results);
		
		// Add another one
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:33:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(2, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, log.getStatus());
				assertEquals(3, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
	}
	
	@Test
	public void testSameAlertTwice() throws Exception {
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:30:55"));
		
		// There should be one
		List<AlertLog> results = DbHelper.getAllInAlertTable();
		assertEquals(1, results.size());
		assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, results.get(0).getStatus());
		DbHelper.printLogs(results);
		
		// Add another one
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:33:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(2, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, log.getStatus());
				assertEquals(3, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
		
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:35:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(3, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(0, log.getDuration());
			} else if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:33:55")){
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(3, log.getDuration());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, log.getStatus());
				assertEquals(5, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
	}
	
	@Test
	public void testSameAlertTwiceAndClear() throws Exception {
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:30:55"));
		
		// There should be one
		List<AlertLog> results = DbHelper.getAllInAlertTable();
		assertEquals(1, results.size());
		assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, results.get(0).getStatus());
		DbHelper.printLogs(results);
		
		// Add another one
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:33:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(2, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, log.getStatus());
				assertEquals(3, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
		
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:35:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(3, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(0, log.getDuration());
			} else if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:33:55")){
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(3, log.getDuration());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, log.getStatus());
				assertEquals(5, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
		
		sendData(createEntry(AlarmAction.CLEARED, "2017-03-27 04:40:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(4, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(0, log.getDuration());
			} else if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:33:55")){
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(3, log.getDuration());
			} else if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:35:55")){
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(5, log.getDuration());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_INACTIVE, log.getStatus());
				assertEquals(10, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
	}
	
	@Test
	public void testSameAlertClearTwice() throws Exception {
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:30:55"));
		
		// There should be one
		List<AlertLog> results = DbHelper.getAllInAlertTable();
		assertEquals(1, results.size());
		assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, results.get(0).getStatus());
		DbHelper.printLogs(results);
		
		// Add another one
		sendData(createEntry(AlarmAction.CLEARED, "2017-03-27 04:33:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(2, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_INACTIVE, log.getStatus());
				assertEquals(3, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
		
		// Should ignore the clear if there's no active before.
		sendData(createEntry(AlarmAction.CLEARED, "2017-03-27 04:35:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(2, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(0, log.getDuration());
			} else if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:33:55")){
				assertEquals(AlarmConstants.ALERT_STATUS_INACTIVE, log.getStatus());
				assertEquals(3, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
	}
	
	@Test
	public void testAlertClearAndRaise() throws Exception {
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:30:55"));
		
		// There should be one
		List<AlertLog> results = DbHelper.getAllInAlertTable();
		assertEquals(1, results.size());
		assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, results.get(0).getStatus());
		DbHelper.printLogs(results);
		
		// Add another one
		sendData(createEntry(AlarmAction.CLEARED, "2017-03-27 04:33:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(2, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_INACTIVE, log.getStatus());
				assertEquals(3, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
		
		// Should ignore the clear if there's no active before.
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:35:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(3, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
				assertEquals(0, log.getDuration());
			} else if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:33:55")){
				assertEquals(AlarmConstants.ALERT_STATUS_INACTIVE, log.getStatus());
				assertEquals(3, log.getDuration());
			} else if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:35:55")){
				assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, log.getStatus());
				assertEquals(0, log.getDuration());
			}
			
		}
		DbHelper.printLogs(results);
	}
	
	@Test
	public void testAlertClearAndRaiseDifferentBucket() throws Exception {
		sendData(createEntry(AlarmAction.RAISED, "2017-03-27 04:30:55"));
		
		// There should be one
		List<AlertLog> results = DbHelper.getAllInAlertTable();
		assertEquals(1, results.size());
		assertEquals(AlarmConstants.ALERT_STATUS_ACTIVE, results.get(0).getStatus());
		DbHelper.printLogs(results);
		
		// Add another one
		sendData(createEntry(AlarmAction.CLEARED, "2017-03-27 05:33:55"));
		results = DbHelper.getAllInAlertTable();
		assertEquals(2, results.size());
		for (AlertLog log : results) {
			if (DTF.print(log.getAlert_ts().getTime()).equals("2017-03-27 04:30:55")) {
				assertEquals(AlarmConstants.ALERT_STATUS_PAST, log.getStatus());
			} else {
				assertEquals(AlarmConstants.ALERT_STATUS_INACTIVE, log.getStatus());
				assertEquals(63, log.getDuration());
			}
		}
		DbHelper.printLogs(results);
	}
}
