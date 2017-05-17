package com.ge.current.em.analytics.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ge.current.ie.email.model.EmailMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.oauth2.client.DefaultOAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.token.grant.client.ClientCredentialsResourceDetails;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

import static com.ge.current.ie.email.model.EmailMessage.EmailMessageBuilder;
import static com.ge.current.ie.email.model.Recipient.RecipientBuilder;
import static com.ge.current.ie.email.model.Recipients.RecipientsBuilder;

@Component public class EmailService {

    @Value("${uaa.token.url}") private String oauthTokenUrl;

    @Value("${uaa.client.id}") private String oauthClientId;

    @Value("${uaa.client.secret}") private String oauthClientSecret;

    @Value("${email.service.url}") private String emailServiceUrl;

    @Value("${email.service.from}") private String emailFrom;

    @Value("${email.service.to}") private String emailTo;

    private static RestTemplate restTemplate;
    private OAuth2RestTemplate oauthTemplate;


    public OAuth2RestTemplate getOauthTemplate() {
        SimpleClientHttpRequestFactory rf = new SimpleClientHttpRequestFactory();
        rf.setConnectTimeout(5 * 1000);
        rf.setReadTimeout(5 * 1000);
        restTemplate = new RestTemplate(rf);

        ClientCredentialsResourceDetails resourceDetails = new ClientCredentialsResourceDetails();
        resourceDetails.setClientId(oauthClientId);
        resourceDetails.setClientSecret(oauthClientSecret);
        resourceDetails.setId(oauthClientId);
        resourceDetails.setAccessTokenUri(oauthTokenUrl);
        System.out.println("*** Create Oauth Template: oauthClientId = " + oauthClientId + " oauthClientSecret = "
                + oauthClientSecret + " oauthToeknUrl = " + oauthTokenUrl);
        oauthTemplate = new OAuth2RestTemplate(resourceDetails, new DefaultOAuth2ClientContext());
        return oauthTemplate;
    }

    public void sendMessage(String appName, String appId, Object errorMessage, Object excStackTrace, String subject) {

        //String emailEnabled = env.getProperty("alert.email.enabled");
        System.out.println("No Alert email needed...");
        /*
            Map<String, Object> params = getMessageBodyMap(appName, appId, 
                                errorMessage, excStackTrace);
            sendEmail(subject, params, emailServiceUrl, emailFrom, emailTo);
        */
    }
    
    public void sendMessage(String appName, String appId, Object errorMessage, Object excStackTrace, String emailServUrl, String subject,
            String emailSender, String emailReceiver) {
        Map<String, Object> params = getMessageBodyMap(appName, appId, errorMessage, excStackTrace);

        sendEmail(subject, params, emailServUrl, emailSender, emailReceiver);

    }
    
    private Map<String, Object> getMessageBodyMap(String appName, String appId, Object errorMessage,
            Object excStackTrace) {
        Map<String, Object> params = new HashMap();
        params.put("appName", appName);
        params.put("appId", appId);
        params.put("excMsg", errorMessage);
        params.put("excStackTrace", excStackTrace);
        return params;
    }

    private void sendEmail(String subject, Map<String, Object> params, String emailServiceUrl, String emailSender, String emailReceiver) {
        String messageBody = createEmailMessage(emailSender, emailReceiver, subject, params);
        if (messageBody == null)
            return;

        System.out.println("*** Sending Email 1111 ****");

        HttpHeaders headers = new HttpHeaders();
        String accessToken = getSecuredToken();
        headers.set("Authorization", "Bearer " + accessToken);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<String>(messageBody, headers);
        ResponseEntity response = restTemplate.postForEntity(emailServiceUrl, entity, ResponseEntity.class);

        if (response.getStatusCode() == HttpStatus.ACCEPTED) {
            System.out.println("Successfully sent an email message.");
        } else {
            System.out.println("Status code = " + response.getStatusCode());
        }

        System.out.println("*** Done with sending Email ****");
    }

    public String createEmailMessage(String from, String to, String subject, Map<String, Object> params) {
        EmailMessage message = EmailMessageBuilder.anEmailMessage()
                .withFrom(RecipientBuilder.aRecipient().withName("Spark Job").withEmail(from).build()).withRecipients(
                        RecipientsBuilder.aRecipients()
                                .withTo(RecipientBuilder.aRecipient().withName("Spark Dev list").withEmail(to).build())
                                .build()).withSubject(subject).withParams(params).build();

        ObjectMapper objMapper = new ObjectMapper();
        String messageBody = null;
        try {
            messageBody = objMapper.writeValueAsString(message);
            System.out.println("Email message: " + messageBody);
            return messageBody;
        } catch (JsonProcessingException e) {
            System.out.println("Failed generate email messages. Reason: " + e.getMessage());
            return null;
        }
    }

    public static RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public String getSecuredToken() {
        if (oauthTemplate == null) {
            oauthTemplate = getOauthTemplate();
        }

        String token = oauthTemplate.getAccessToken().getValue();
        System.out.println("*** token = " + token);
        return token;
    }
}
