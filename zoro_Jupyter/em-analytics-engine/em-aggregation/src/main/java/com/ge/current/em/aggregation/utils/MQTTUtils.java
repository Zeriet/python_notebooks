//package com.ge.current.em.aggregation.utils;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import org.eclipse.paho.client.mqttv3.MqttClient;
//import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
//import org.eclipse.paho.client.mqttv3.MqttException;
//import org.eclipse.paho.client.mqttv3.MqttMessage;
//import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
//import org.eclipse.paho.client.mqttv3.MqttSecurityException;
//import java.io.Serializable;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.TimeZone;
//import java.util.List;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.text.SimpleDateFormat;
//
//import com.ge.current.em.aggregation.;
//import com.ge.current.em.analytics.dto.PointDTO;
//import com.ge.current.em.analytics.dto.ReadingsDTO;
//
//public class MQTTUtils implements Serializable{
//    private static final long serialVersionUID=1L;
//
//    public static MqttClient mqttClient = null;
//    public static final String mqttUrl = "tcp://localhost:1883";
//
//    public static ObjectMapper OBJ_MAPPER = new ObjectMapper();
//
//    public static boolean createConnection() {
//
//      try {
//        mqttClient = new MqttClient(mqttUrl, MqttClient.generateClientId());
//      } catch (Exception e) {
//        System.out.println("Could not create mq client, error {}" +  e.getMessage());
//        return false;
//      }
//
//      MqttConnectOptions connOpts = new MqttConnectOptions();
//      connOpts.setCleanSession(true);
//      connOpts.setUserName("test");
//      connOpts.setPassword("test".toCharArray());
//      System.out.println("Finished setting up client connection options");
//
//      try {
//          mqttClient.connect(connOpts);
//        } catch (MqttSecurityException mqse) {
//          System.out.println("Could not connect to mq due to security exception, error {}" + mqse.getMessage());
//          return false;
//        } catch (MqttException mqe) {
//          System.out.println("Could not connect to mq, error {}" + mqe.getMessage());
//          return false;
//        }
//        return true;
//     }
//
//      public static boolean publishMessage(String queueName, String messageBody) {
//        System.out.println("Publishing message to URL: " + mqttUrl + " queue: " + queueName);
//        System.out.println("==== Message: " + messageBody);
//        if (mqttClient == null && !createConnection()) {
//          return false;
//        }
//
//        if (!mqttClient.isConnected()) {
//          try {
//            mqttClient.connect();
//          } catch (Exception e) {
//            System.out.println("Could not connect mq client, error {}" + e.getMessage());
//            return false;
//          }
//        }
//        try {
//            //String messageStr = OBJ_MAPPER.writeValueAsString(eventDetails);
//            //System.out.println(messageStr);
//            mqttClient.publish(queueName, new MqttMessage(messageBody.getBytes()));
//          } catch (MqttPersistenceException mqpe) {
//            System.out.println("Could not publish message due to persistence problem, error {}" + mqpe.getMessage());
//            return false;
//          }
//          catch (MqttException mqe) {
//            System.out.println("Could not publish message, error {}" + mqe.getMessage());
//            return false;
//          }
//          System.out.println("Successfully published message to URL: " + mqttUrl);
//          return true;
//        }
//
//        public static void main(String[] args) throws Exception {
//            System.out.println("====== Enter Main =======");
//            int lag =0;
//            if(args != null && args.length >0){
//            	lag = Integer.parseInt(args[0]);
//            }
//            Map<String, Object> map = new HashMap<>();
//            List<ReadingsDTO> readings = new ArrayList<>();
//            int roomTemp = 60;
//
//            //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
//            SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yy h:mm a z");
//            for(int i = 0; i < 1; i++) {
//                roomTemp = roomTemp + i*10;
//                Calendar cal = Calendar.getInstance();
//                cal.add(Calendar.MINUTE, -lag);
//                String timeString = sdf.format(cal.getTime());
//                ReadingsDTO readingDTO = new ReadingsDTO("OK", roomTemp, timeString);
//                readings.add(readingDTO);
//                PointDTO point = PointDTO.PointDTOBuilder.aPointDTO() PointDTO("BacnetNetwork/AHU1/RoomTemp", roomTemp, "ok", "Numeric", readings);
//                List<PointDTO> points = new ArrayList<>();
//                points.add(point);
//                PointDTO point2 = new PointDTO("BacnetNetwork/AHU1/FanStat", "true", "ok", "Boolean", readings);
//                points.add(point2);
//                EquipDTO equipDTO = new EquipDTO("BacnetNetwork/AHU1",
//                        "ASSET_986bd75d-a7cb-3150-b59c-f40760c0b432", timeString, 300, points);
//                try {
//                    publishMessage("jace.event", new ObjectMapper().writeValueAsString(equipDTO));
//                } catch (Exception e) {
//                    System.out.println("*** Exception for publish JSON ***");
//                }
//
//                Thread.sleep(1000);
//            }
//            System.out.println("==== Done with 10 Messages ====");
//        }
//
//}
//
//
