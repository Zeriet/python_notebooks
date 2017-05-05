/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.streaming.receivers;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLSocketFactory;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import com.ge.current.ie.spark.ssl.SslSocketFactoryConfig;

public abstract class AbstractRabbitMqReceiver<T> extends Receiver<T> implements ChannelAwareMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitMqReceiver.class);

    private List<String> queues = new ArrayList<>();
    private SslSocketFactoryConfig sslSocketFactoryConfig;
    private URI uri;
    private int concurrentConsumers = 2;

    private transient SimpleMessageListenerContainer container;

    public AbstractRabbitMqReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onStart() {
        try {
            ConnectionFactory connectionFactory = createConnectionFactory();

            container = new SimpleMessageListenerContainer(connectionFactory);
            queues.forEach(q -> container.addQueueNames(q));
            container.setConcurrentConsumers(concurrentConsumers);
            container.setMessageListener(this);
            container.setAutoDeclare(false);
            container.start();
            LOGGER.info("RabbitMQ receiver started on queues {}", queues);
        } catch (Exception e) {
            LOGGER.warn("RabbitMQ receiver failed to start", e);
            restart("RabbitMQ receiver failed to start", e);
        }
    }

    private ConnectionFactory createConnectionFactory() throws Exception {
        RabbitConnectionFactoryBean connectionFactoryBean = new RabbitConnectionFactoryBean();
        connectionFactoryBean.setUri(uri);

        if (sslSocketFactoryConfig != null) {
            connectionFactoryBean.setUseSSL(true);
            SSLSocketFactory sslSocketFactory = sslSocketFactoryConfig.create();
            connectionFactoryBean.setSocketFactory(sslSocketFactory);
        }

        connectionFactoryBean.afterPropertiesSet();
        return new CachingConnectionFactory(connectionFactoryBean.getObject());
    }

    @Override
    public void onStop() {
        container.stop();
    }

    public void addQueue(String queue) {
        queues.add(queue);
    }

    public void setQueues(List<String> queues) {
        this.queues = queues;
    }

    public AbstractRabbitMqReceiver setSslSocketFactoryConfig(SslSocketFactoryConfig sslSocketFactoryConfig) {
        this.sslSocketFactoryConfig = sslSocketFactoryConfig;
        return this;
    }

    public AbstractRabbitMqReceiver setUri(URI uri) {
        this.uri = uri;
        return this;
    }

    public AbstractRabbitMqReceiver setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
        return this;
    }
}
