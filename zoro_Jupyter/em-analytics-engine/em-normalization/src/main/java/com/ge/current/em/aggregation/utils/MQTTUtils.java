package com.ge.current.em.aggregation.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.ge.current.em.analytics.dto.PointDTO;
import com.ge.current.em.analytics.dto.PointReadingDTO;
import com.ge.current.em.analytics.dto.SiteReadingDTO;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

import org.apache.commons.io.IOUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.List;
import java.util.ArrayList;
import java.util.Calendar;
import java.text.SimpleDateFormat;


public class MQTTUtils implements Serializable {
    private static final long serialVersionUID = 1L;

    public static MqttClient mqttClient = null;
    public static final String mqttUrl = "tcp://localhost:1883";

    public static ObjectMapper OBJ_MAPPER = new ObjectMapper();

    public static boolean createConnection() {

        try {
            mqttClient = new MqttClient(mqttUrl, MqttClient.generateClientId());
        } catch (Exception e) {
            System.out.println("Could not create mq client, error {}" + e.getMessage());
            return false;
        }

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);

        System.out.println("Finished setting up client connection options");

        try {
            mqttClient.connect(connOpts);
        } catch (MqttSecurityException mqse) {
            System.out.println("Could not connect to mq due to security exception, error {}" + mqse.getMessage());
            return false;
        } catch (MqttException mqe) {
            System.out.println("Could not connect to mq, error {}" + mqe.getMessage());
            return false;
        }
        return true;
    }

    public static boolean publishMessage(String queueName, String messageBody) {
        System.out.println("Publishing message to URL: " + mqttUrl + " queue: " + queueName);
        System.out.println("==== Message: " + messageBody);
        if (mqttClient == null && !createConnection()) {
            return false;
        }

        if (!mqttClient.isConnected()) {
            try {
                mqttClient.connect();
            } catch (Exception e) {
                System.out.println("Could not connect mq client, error {}" + e.getMessage());
                return false;
            }
        }
        try {
            //String messageStr = OBJ_MAPPER.writeValueAsString(eventDetails);
            //System.out.println(messageStr);
            mqttClient.publish(queueName, new MqttMessage(messageBody.getBytes()));
        } catch (MqttPersistenceException mqpe) {
            System.out.println("Could not publish message due to persistence problem, error {}" + mqpe.getMessage());
            return false;
        } catch (MqttException mqe) {
            System.out.println("Could not publish message, error {}" + mqe.getMessage());
            return false;
        }
        System.out.println("Successfully published message to URL: " + mqttUrl);
        return true;
    }

    public static void main(String[] args) {
        System.out.println("====== Enter Main =======");
        int lag = 0;
        if (args != null && args.length > 0) {
            lag = Integer.parseInt(args[0]);
        }
        Map<String, Object> map = new HashMap<>();
        SiteReadingDTO siteReading = new SiteReadingDTO();
        siteReading.setEdgeDeviceId("gateway_pragna");
        siteReading.setEdgeDeviceType("JACE");
        siteReading.setReportTime("2017-01-05T14:25:30.676-08:00 America/Los_Angeles");
        siteReading.setReportingIntervalInSeconds(300);

        List<PointDTO> points = new ArrayList();


        int roomTemp = 60;

        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yy h:mm a z");
        String[] pointList = new String[] {"/Drivers/BacnetNetwork/AHU_002/points/zoneAirTempEffectiveSp"};
        for (int i = 0; i < pointList.length; i++) {
            PointDTO point = new PointDTO();
            roomTemp = roomTemp + i * 10;
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MINUTE, -lag);
            String timeString = sdf.format(cal.getTime());

            point.setPointName(pointList[i]);
            point.setCurValue("On");
            point.setCurStatus("OK");
            point.setPointType("Boolean");

            List<PointReadingDTO> pointReadings = new ArrayList<>();
            pointReadings.add(new PointReadingDTO("OK", "On", "2017-01-05T14:24:17.676-08:00"));
            pointReadings.add(new PointReadingDTO("OK", "Off", "2017-01-05T14:24:32.695-08:00"));
            pointReadings.add(new PointReadingDTO("OK", "On", "2017-01-05T14:24:47.744-08:00"));
            pointReadings.add(new PointReadingDTO("OK", "Off", "2017-01-05T14:25:02.748-08:00"));

            point.setPointReadings(pointReadings);
            points.add(point);
        }

        siteReading.setPoints(points);

        try {
            publishMessage("jace.event", IOUtils.toString(MQTTUtils.class.getResourceAsStream("/SampleJACE.json")));
        } catch (Exception e) {
            System.out.println("*** Exception for publish JSON ***");
        }
        System.out.println("==== Done with sending  Messages ====");
        try {
            mqttClient.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
