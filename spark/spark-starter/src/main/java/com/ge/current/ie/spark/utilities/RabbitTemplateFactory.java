/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.utilities;

import static com.ge.current.ie.spark.utilities.SparkConfFunctions.supplyMissingConfigException;

import java.io.Serializable;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.spark.SparkConf;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.ge.current.ie.spark.ssl.SslSocketFactoryConfig;

/**
 * Utility class to store and load rabbit templates for execution
 * by spark workers.
 */
public class RabbitTemplateFactory implements Serializable {

    private transient LoadingCache<Configuration, RabbitTemplate> templateCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<Configuration, RabbitTemplate>() {
                @Override
                public RabbitTemplate load(Configuration configuration) throws Exception {
                    return createRabbitTemplate(configuration);
                }
            });

    private RabbitTemplate createRabbitTemplate(Configuration configuration) throws Exception {
        ConnectionFactory connectionFactory = createConnectionFactory(configuration);
        return new RabbitTemplate(connectionFactory);
    }

    private ConnectionFactory createConnectionFactory(Configuration configuration) throws Exception {
        RabbitConnectionFactoryBean connectionFactoryBean = new RabbitConnectionFactoryBean();
        connectionFactoryBean.setUri(configuration.getURI());

        if (configuration.getSslSocketFactoryConfig().isPresent()) {
            connectionFactoryBean.setUseSSL(true);
            SSLSocketFactory sslSocketFactory = configuration.getSslSocketFactoryConfig().get().create();
            connectionFactoryBean.setSocketFactory(sslSocketFactory);
        }

        connectionFactoryBean.afterPropertiesSet();
        return new CachingConnectionFactory(connectionFactoryBean.getObject());
    }

    public RabbitTemplate get(Configuration configuration) {
        try {
            return templateCache.get(configuration);
        } catch (ExecutionException e) {
            throw new RuntimeException("Could not get rabbit template.", e);
        }
    }

    public static class Configuration implements Serializable {

        private static final String SC_PROP_RABBITMQ_URI                        = "ie.spark.rabbitmq.uri";
        private static final String SC_PROP_RABBITMQ_SSL_ENABLED                = "ie.spark.rabbitmq.ssl.enabled";
        private static final String SC_PROP_RABBITMQ_SSL_TLS_VERSION            = "ie.spark.rabbitmq.ssl.tlsVersion";
        private static final String SC_PROP_RABBITMQ_SSL_KEYSTORE               = "ie.spark.rabbitmq.ssl.keystore";
        private static final String SC_PROP_RABBITMQ_SSL_KEYSTORE_TYPE          = "ie.spark.rabbitmq.ssl.keystoreType";
        private static final String SC_PROP_RABBITMQ_SSL_KEYSTORE_PASSWORD      = "ie.spark.rabbitmq.ssl.keystorePassword";
        private static final String SC_PROP_RABBITMQ_SSL_TRUSTSTORE             = "ie.spark.rabbitmq.ssl.truststore";
        private static final String SC_PROP_RABBITMQ_SSL_TRUSTSTORE_TYPE        = "ie.spark.rabbitmq.ssl.truststoreType";
        private static final String SC_PROP_RABBITMQ_SSL_TRUSTSTORE_PASSWORD    = "ie.spark.rabbitmq.ssl.truststorePassword";

        private URI uri;
        private SslSocketFactoryConfig sslSocketFactoryConfig;

        public Configuration apply(SparkConf sparkConf) {

            uri = SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_URI)
                    .map(s -> URI.create(s))
                    .orElseThrow(supplyMissingConfigException(SC_PROP_RABBITMQ_URI));


            Optional<Boolean> sslEnabled = SparkConfFunctions.getBool(sparkConf, SC_PROP_RABBITMQ_SSL_ENABLED);
            if (sslEnabled.isPresent() && sslEnabled.get()) {
                SslSocketFactoryConfig.SslSocketFactoryConfigBuilder builder = SslSocketFactoryConfig.defaults();

                builder.withTlsVersion(SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_SSL_TLS_VERSION)
                    .orElse(SslSocketFactoryConfig.DEFAULT_TLS_VERSION));
                
                builder.withKeyStorePath(SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_SSL_KEYSTORE)
                    .map(s -> URI.create(s))
                    .orElseThrow(supplyMissingConfigException(SC_PROP_RABBITMQ_SSL_KEYSTORE)));
               
                builder.withKeyStorePassword(SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_SSL_KEYSTORE_PASSWORD)
                    .orElse(SslSocketFactoryConfig.DEFAULT_PASSWORD));
                
                builder.withKeyStoreType(SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_SSL_KEYSTORE_TYPE)
                    .orElse(SslSocketFactoryConfig.DEFAULT_KEYSTORE_TYPE));
                 
                builder.withTrustStorePath(SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_SSL_TRUSTSTORE)
                    .map(s -> URI.create(s))
                    .orElseThrow(supplyMissingConfigException(SC_PROP_RABBITMQ_SSL_TRUSTSTORE)));
               
                builder.withTrustStorePassword(SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_SSL_TRUSTSTORE_PASSWORD)
                    .orElse(SslSocketFactoryConfig.DEFAULT_PASSWORD));
                
                builder.withTrustStoreType(SparkConfFunctions.get(sparkConf, SC_PROP_RABBITMQ_SSL_TRUSTSTORE_TYPE)
                    .orElse(SslSocketFactoryConfig.DEFAULT_TRUSTSTORE_TYPE));

                sslSocketFactoryConfig = builder.build();
            }

            return this;
        }

        public URI getURI() {
            return uri;
        }

        public Optional<SslSocketFactoryConfig> getSslSocketFactoryConfig() {
            return Optional.ofNullable(sslSocketFactoryConfig);
        }
    }
}
