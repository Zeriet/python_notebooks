/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.utilities;

import static com.ge.current.ie.email.model.EmailMessage.EmailMessageBuilder.anEmailMessage;
import static com.ge.current.ie.email.model.Recipients.RecipientsBuilder.aRecipients;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.spark.SparkConf;
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

import com.ge.current.ie.email.model.EmailMessage;
import com.ge.current.ie.email.model.Recipient;

/**
 * Easy to use, serializable template to send emails
 * using the platform email service.
 *
 */
public class EmailTemplate implements Serializable {

    private final Configuration configuration;
    private Recipient to;
    private Recipient from;
    private String subject;
    private String template;
    private Map<String, Object> properties;
    private transient RestTemplate restTemplate;

    public EmailTemplate(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration);
    }

    protected void send(String template, EmailMessage emailMessage) {
        setUpRestTemplate();

        String url = configuration.getUrl() + "/v2/email/{template}";

        try {
            restTemplate.postForEntity(url, emailMessage, Void.class, template);
        } catch (HttpStatusCodeException e) {
            throw new EmailException("Unable to send email.", e);
        }
    }

    public Builder newBuilder() {
        Builder builder = new Builder(this);

        if (subject != null) {
            builder.setSubject(subject);
        }

        if (template != null) {
            builder.setTemplate(template);
        }

        if (to != null) {
            builder.setTo(to.getName(), to.getEmail());
        }

        if (from != null) {
            builder.setFrom(from.getName(), from.getEmail());
        }
        if (properties != null) {
            properties.entrySet().forEach(e -> builder.addProperty(e.getKey(), e.getValue()));
        }
        return builder;
    }

    /**
     * Email Builder class. Provides an easy interface for creating an email message from
     * an email template and sending it. Builders are not threadsafe, and should not be shared.
     * Builders can be reused.
     */
    public class Builder implements Serializable {
        private final EmailTemplate emailTemplate;
        private String subject;
        private String template;
        private Recipient to;
        private Recipient from;
        private Map<String, Object> properties = new HashMap<>();

        public Builder(EmailTemplate emailTemplate) {
            this.emailTemplate = emailTemplate;
        }

        public Builder setTemplate(String template) {
            this.template = Objects.requireNonNull(template);
            return this;
        }

        public Builder setTo(String name, String email) {
            this.to = new Recipient()
                    .setEmail(Objects.requireNonNull(email))
                    .setName(Objects.requireNonNull(name));
            return this;
        }

        public Builder setFrom(String name, String email) {
            this.from = new Recipient()
                    .setEmail(Objects.requireNonNull(email))
                    .setName(Objects.requireNonNull(name));
            return this;
        }

        public Builder addProperty(String property, Object value) {
            properties.put(property, value);
            return this;
        }

        public Builder setSubject(String subject) {
            this.subject = subject;
            return this;
        }

        public void send() {
            validate();
            emailTemplate.send(template, anEmailMessage()
                    .withRecipients(aRecipients()
                            .withTo(to)
                            .build())
                    .withFrom(from)
                    .withParams(properties)
                    .withSubject(subject)
                    .build());
        }

        private void validate() {
            if (template == null || template.isEmpty()) {
                throw new EmailException("Template is null or empty.");
            }
            if (subject == null || subject.isEmpty()) {
                throw new EmailException("Subject is null or empty.");
            }
            if (to == null) {
                throw new EmailException("No recipient to send to.");
            }
            if (from == null) {
                throw new EmailException("No recipient to send from.");
            }
        }
    }

    private void setUpRestTemplate() {
        if (restTemplate != null) {
            return;
        }

        AccessTokenRequest atr = new DefaultAccessTokenRequest();
        OAuth2RestTemplate restTemplate = new OAuth2RestTemplate(getResourceDetails(), new DefaultOAuth2ClientContext(atr));

        if (configuration.hasProxy()) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(configuration.getProxyHost(), configuration.getProxyPort()));
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
        resourceDetails.setAccessTokenUri(configuration.getIssuerUrl());
        resourceDetails.setClientId(configuration.getClientId());
        resourceDetails.setClientSecret(configuration.getClientSecret());
        resourceDetails.setScope(configuration.getScopes());
        return resourceDetails;
    }

    public static class Configuration implements Serializable {

        private static final String SC_PROP_EMAIL_URL_KEY           = "ie.spark.email.url";
        private static final String SC_PROP_EMAIL_CLIENT_ID_KEY     = "ie.spark.email.auth.clientId";
        private static final String SC_PROP_EMAIL_CLIENT_SECRET_KEY = "ie.spark.email.auth.clientSecret";
        private static final String SC_PROP_EMAIL_ISSUER_URL        = "ie.spark.email.auth.issuerUrl";
        private static final String SC_PROP_EMAIL_SCOPES            = "ie.spark.email.auth.scopes";

        private String url;
        private String clientId;
        private String clientSecret;
        private String issuerUrl;
        private String proxyHost;
        private int proxyPort;
        private List<String> scopes;

        public Configuration apply(SparkConf sparkConf) {
            url = SparkConfFunctions.get(sparkConf, SC_PROP_EMAIL_URL_KEY)
                    .orElseThrow(supplyMissingConfigException(SC_PROP_EMAIL_URL_KEY));

            clientId = SparkConfFunctions.get(sparkConf, SC_PROP_EMAIL_CLIENT_ID_KEY)
                    .orElseThrow(supplyMissingConfigException(SC_PROP_EMAIL_CLIENT_ID_KEY));

            clientSecret = SparkConfFunctions.get(sparkConf, SC_PROP_EMAIL_CLIENT_SECRET_KEY)
                    .orElseThrow(supplyMissingConfigException(SC_PROP_EMAIL_CLIENT_SECRET_KEY));

            issuerUrl = SparkConfFunctions.get(sparkConf, SC_PROP_EMAIL_ISSUER_URL)
                    .orElseThrow(supplyMissingConfigException(SC_PROP_EMAIL_ISSUER_URL));

            scopes = SparkConfFunctions.getList(sparkConf, SC_PROP_EMAIL_SCOPES);
            if (scopes.isEmpty()) {
                throw supplyMissingConfigException(SC_PROP_EMAIL_SCOPES).get();
            }

            proxyHost = SparkConfFunctions.get(sparkConf, "http.proxyHost").orElse(null);
            proxyPort = SparkConfFunctions.getInt(sparkConf, "http.proxyPort").orElse(80);

            return this;
        }

        private static Supplier<ConfigurationException> supplyMissingConfigException(String property) {
            return () -> new ConfigurationException("Missing property %s inside Spark config.", property);
        }

        public String getUrl() {
            return url;
        }

        public Configuration setUrl(String url) {
            this.url = url;
            return this;
        }

        public String getClientId() {
            return clientId;
        }

        public Configuration setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public String getClientSecret() {
            return clientSecret;
        }

        public Configuration setClientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        public String getIssuerUrl() {
            return issuerUrl;
        }

        public Configuration setIssuerUrl(String issuerUrl) {
            this.issuerUrl = issuerUrl;
            return this;
        }

        public List<String> getScopes() {
            return scopes;
        }

        public Configuration setScopes(List<String> scopes) {
            this.scopes = scopes;
            return this;
        }

        public String getProxyHost() {
            return proxyHost;
        }

        public int getProxyPort() {
            return proxyPort;
        }

        public boolean hasProxy() {
            return proxyHost != null && !proxyHost.isEmpty();
        }
    }

    public EmailTemplate setFrom(Recipient from) {
        this.from = from;
        return this;
    }

    public EmailTemplate setFrom(String name, String email) {
        return setFrom(new Recipient()
                .setEmail(Objects.requireNonNull(email))
                .setName(Objects.requireNonNull(name)));
    }

    public EmailTemplate setTo(Recipient to) {
        this.to = to;
        return this;
    }

    public EmailTemplate setTo(String name, String email) {
        return setTo(new Recipient()
                .setEmail(Objects.requireNonNull(email))
                .setName(Objects.requireNonNull(name)));
    }

    public EmailTemplate setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public EmailTemplate setTemplate(String template) {
        this.template = template;
        return this;
    }

    public EmailTemplate setProperties(Map<String, Object> properties) {
        this.properties = properties;
        return this;
    }
}
