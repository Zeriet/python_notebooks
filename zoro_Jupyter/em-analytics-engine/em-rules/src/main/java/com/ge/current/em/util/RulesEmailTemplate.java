package com.ge.current.em.util;

import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;
import com.ge.current.ie.email.model.EmailMessage;
import com.ge.current.ie.email.model.Recipient;
import com.ge.current.ie.email.model.Recipients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.oauth2.client.DefaultOAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.resource.OAuth2ProtectedResourceDetails;
import org.springframework.security.oauth2.client.token.AccessTokenRequest;
import org.springframework.security.oauth2.client.token.DefaultAccessTokenRequest;
import org.springframework.security.oauth2.client.token.grant.client.ClientCredentialsAccessTokenProvider;
import org.springframework.security.oauth2.client.token.grant.client.ClientCredentialsResourceDetails;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by 212577826 on 4/20/17.
 */
public class RulesEmailTemplate implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(RulesEmailTemplate.class);
    private PropertiesBean propertiesBean;
    private transient RestTemplate restTemplate;

    public RulesEmailTemplate(PropertiesBean propertiesBean) {
        this.propertiesBean = Objects.requireNonNull(propertiesBean);
    }

    public void sendEmail(String appName, String appId, String errorMessage, String excStackTrace) {

        if( !Boolean.valueOf(this.propertiesBean.getValue(SparkConfigurations.ERROR_EMAIL_ENABLED))) {
            LOG.info(" === Error email is not enabled === ");
        } else{
            setUpRestTemplate();
            String url = this.propertiesBean.getValue(SparkConfigurations.ERROR_EMAIL_ENDPOINT);
            Map<String, Object> messageBody = getMessageBodyMap(appName, appId, errorMessage, excStackTrace);
            String emailMessage = createEmailMessage(propertiesBean.getValue(SparkConfigurations.ERROR_EMAIL_FROM),
                    propertiesBean.getValue(SparkConfigurations.ERROR_EMAIL_TO), "Rule job Failed", messageBody);
            try {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<String> entity = new HttpEntity<>(emailMessage, headers);
                ResponseEntity response = restTemplate.postForEntity(url, entity, ResponseEntity.class);
                if (response.getStatusCode() == HttpStatus.ACCEPTED) {
                    LOG.info("Successfully sent an email message.");
                } else {
                    LOG.info("Status code = " + response.getStatusCode());
                }
            } catch (HttpStatusCodeException e) {
                e.printStackTrace();
            }
        }
    }

    private void setUpRestTemplate() {
        if (restTemplate != null) {
            return;
        }

        AccessTokenRequest atr = new DefaultAccessTokenRequest();
        OAuth2RestTemplate restTemplate = new OAuth2RestTemplate(getResourceDetails(),
                new DefaultOAuth2ClientContext(atr));

        if (Boolean.valueOf(propertiesBean.getValue("app.proxy.enabled"))) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP,
                    new InetSocketAddress(propertiesBean.getValue(SparkConfigurations.APP_PROXY_HOST),
                            Integer.valueOf(propertiesBean.getValue(SparkConfigurations.APP_PROXY_PORT))));
            SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
            requestFactory.setProxy(proxy);

            ClientCredentialsAccessTokenProvider provider = new ClientCredentialsAccessTokenProvider();
            provider.setRequestFactory(requestFactory);
            restTemplate.setAccessTokenProvider(provider);
            restTemplate.setRequestFactory(requestFactory);

        }
        this.restTemplate = restTemplate;
    }

    private OAuth2ProtectedResourceDetails getResourceDetails() {
        ClientCredentialsResourceDetails resourceDetails = new ClientCredentialsResourceDetails();
        resourceDetails.setAccessTokenUri(propertiesBean.getValue(SparkRulesConfig.Configurations.UAA_TOKEN_URL.getConfigName()));
        resourceDetails.setClientId(propertiesBean.getValue(SparkRulesConfig.Configurations.UAA_CLIENT_ID.getConfigName()));
        resourceDetails.setClientSecret(propertiesBean.getValue(SparkRulesConfig.Configurations.UAA_CLIENT_SECRET.getConfigName()));
        resourceDetails.setScope(Arrays.asList(propertiesBean.getValue(SparkRulesConfig.Configurations.UAA_CLIENT_SCOPE.getConfigName())));
        return resourceDetails;
    }

    private String createEmailMessage(String from, String to, String subject, Map<String, Object> params) {
        EmailMessage message = EmailMessage.EmailMessageBuilder.anEmailMessage()
                .withFrom(Recipient.RecipientBuilder.aRecipient().withName("Spark Job").withEmail(from).build())
                .withRecipients(Recipients.RecipientsBuilder.aRecipients()
                        .withTo(Recipient.RecipientBuilder.aRecipient().withName("Spark Dev list").withEmail(to)
                                .build()).build()).withSubject(subject).withParams(params).build();

        ObjectMapper objMapper = new ObjectMapper();
        String messageBody = null;
        try {
            messageBody = objMapper.writeValueAsString(message);
            return messageBody;
        } catch (JsonProcessingException e) {
            Log.info("Failed to generate email messages. Reason: " + e.getMessage());
            return null;
        }
    }

    private Map<String, Object> getMessageBodyMap(String appName, String appId, String errorMessage,
            String excStackTrace) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("appName", appName);
        params.put("appId", appId);
        params.put("excMsg", errorMessage);
        params.put("excStackTrace", excStackTrace);
        return params;
    }

}
