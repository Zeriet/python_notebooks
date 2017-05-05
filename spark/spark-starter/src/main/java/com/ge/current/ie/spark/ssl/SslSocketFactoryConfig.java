/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.ssl;

import static com.ge.current.ie.spark.ssl.SslSocketFactoryConfig.SslSocketFactoryConfigBuilder.aSslSocketFactoryConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Base64;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class SslSocketFactoryConfig implements Serializable {
    private static final long serialVersionUID = 9102299060470634190L;

    public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";
    public static final String DEFAULT_PASSWORD = "Y2hhbmdlbWU=";
    public static final String DEFAULT_TRUSTSTORE_TYPE = "JKS";
    public static final String DEFAULT_TLS_VERSION = "TLSv1.2";

    private String keyStoreType;
    private String keyStorePassword;
    private URI keyStorePath;

    private String trustStoreType;
    private String trustStorePassword;
    private URI trustStorePath;

    private String tlsVersion;

    public SSLSocketFactory create() throws GeneralSecurityException, IOException {
        SSLContext context = createSSLContext();
        return context.getSocketFactory();
    }

    private SSLContext createSSLContext() throws IOException, GeneralSecurityException {
        char[] keyPassphrase = decodeBase64EncodedString(keyStorePassword).toCharArray();
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        ks.load(getInputStream(keyStorePath), keyPassphrase);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, keyPassphrase);

        char[] trustPassphrase = decodeBase64EncodedString(trustStorePassword).toCharArray();
        KeyStore tks = KeyStore.getInstance(trustStoreType);
        tks.load(getInputStream(trustStorePath), trustPassphrase);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(tks);

        SSLContext c = SSLContext.getInstance(tlsVersion);
        c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return c;
    }

    private InputStream getInputStream(URI uri) throws IOException {
        if (uri.getScheme().equals("classpath")) {
            return SslSocketFactoryConfig.class.getResourceAsStream(uri.getPath());
        }
        return uri.toURL().openStream();
    }

    private String decodeBase64EncodedString(String keyStorePassword) {
        if (keyStorePassword == null || keyStorePassword.isEmpty()) {
            return "";
        }
        return new String(Base64.getDecoder().decode(keyStorePassword));
    }

    public static SslSocketFactoryConfig.SslSocketFactoryConfigBuilder defaults() {
        return aSslSocketFactoryConfig()
                .withKeyStoreType(DEFAULT_KEYSTORE_TYPE)
                .withKeyStorePassword(DEFAULT_PASSWORD)
                .withTrustStoreType(DEFAULT_TRUSTSTORE_TYPE)
                .withTrustStorePassword(DEFAULT_PASSWORD)
                .withTlsVersion(DEFAULT_TLS_VERSION);
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public SslSocketFactoryConfig setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
        return this;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public SslSocketFactoryConfig setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public URI getKeyStorePath() {
        return keyStorePath;
    }

    public SslSocketFactoryConfig setKeyStorePath(URI keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public SslSocketFactoryConfig setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
        return this;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public SslSocketFactoryConfig setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public URI getTrustStorePath() {
        return trustStorePath;
    }

    public SslSocketFactoryConfig setTrustStorePath(URI trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public String getTlsVersion() {
        return tlsVersion;
    }

    public SslSocketFactoryConfig setTlsVersion(String tlsVersion) {
        this.tlsVersion = tlsVersion;
        return this;
    }

    public static final class SslSocketFactoryConfigBuilder {
        private String keyStoreType;
        private String keyStorePassword;
        private URI keyStorePath;
        private String trustStoreType;
        private String trustStorePassword;
        private URI trustStorePath;
        private String tlsVersion;

        private SslSocketFactoryConfigBuilder() {
        }

        public static SslSocketFactoryConfigBuilder aSslSocketFactoryConfig() {
            return new SslSocketFactoryConfigBuilder();
        }

        public SslSocketFactoryConfigBuilder withKeyStoreType(String keyStoreType) {
            this.keyStoreType = keyStoreType;
            return this;
        }

        public SslSocketFactoryConfigBuilder withKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        public SslSocketFactoryConfigBuilder withKeyStorePath(URI keyStorePath) {
            this.keyStorePath = keyStorePath;
            return this;
        }

        public SslSocketFactoryConfigBuilder withTrustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return this;
        }

        public SslSocketFactoryConfigBuilder withTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return this;
        }

        public SslSocketFactoryConfigBuilder withTrustStorePath(URI trustStorePath) {
            this.trustStorePath = trustStorePath;
            return this;
        }

        public SslSocketFactoryConfigBuilder withTlsVersion(String tlsVersion) {
            this.tlsVersion = tlsVersion;
            return this;
        }

        public SslSocketFactoryConfig build() {
            SslSocketFactoryConfig sslSocketFactoryConfig = new SslSocketFactoryConfig();
            sslSocketFactoryConfig.setKeyStoreType(keyStoreType);
            sslSocketFactoryConfig.setKeyStorePassword(keyStorePassword);
            sslSocketFactoryConfig.setKeyStorePath(keyStorePath);
            sslSocketFactoryConfig.setTrustStoreType(trustStoreType);
            sslSocketFactoryConfig.setTrustStorePassword(trustStorePassword);
            sslSocketFactoryConfig.setTrustStorePath(trustStorePath);
            sslSocketFactoryConfig.setTlsVersion(tlsVersion);
            return sslSocketFactoryConfig;
        }
    }
}
