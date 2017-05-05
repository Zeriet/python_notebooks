/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.testing.rule;

import java.net.URI;
import java.util.HashMap;

import com.google.common.io.Files;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.rules.ExternalResource;

public class EmbeddedAmqpBroker extends ExternalResource {

    // Not the standard AMQP port, mainly to avoid conflicts.
    private static final int DEFAULT_PORT = 25672;
    private static final String URI_FORMAT = "amqp://guest:guest@[::1]:%d";

    private final int port;
    private final String configFile;

    private Broker broker;

    private Connection connection;
    private Channel channel;
    private URI uri;

    public EmbeddedAmqpBroker() {
        this("qpid-config.json", DEFAULT_PORT);
    }

    public EmbeddedAmqpBroker(String configFile, int port) {
        this.port = port;
        this.uri = URI.create(String.format(URI_FORMAT, port));
        this.configFile = configFile;
    }

    @Override
    protected void before() throws Throwable {
        broker = new Broker();
        BrokerOptions opts = new BrokerOptions();
        // prepare options
        opts.setConfigProperty("broker.name", "embedded-broker");
        opts.setConfigProperty("qpid.amqp_port", String.valueOf(port));
        opts.setConfigProperty("qpid.work_dir", Files.createTempDir().getAbsolutePath());
        opts.setInitialConfigurationLocation(getClass().getResource(configFile).toString());

        broker.startup(opts);

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(uri);
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
    }

    @Override
    protected void after() {
        try {
            channel.close();
            connection.close();
            broker.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public URI getUri() {
        return uri;
    }

    public void queueDeclare(String queue) throws Exception {
        channel.queueDeclare(queue, true, false, false, new HashMap<>());
    }

    public void exchangeDeclare(String name, String type) throws Exception {
        channel.exchangeDeclare(name, type);
    }

    public void queueBind(String exchange, String queue, String routingKey) throws Exception {
        channel.queueBind(queue, exchange, routingKey);
    }

    public void publish(String exchange, String routingKey, byte[] body) throws Exception {
        publish(exchange, routingKey, body, "text/plain");
    }

    public void publish(String exchange, String routingKey, byte[] body, String contentType) throws Exception {
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType(contentType)
                .build();
        channel.basicPublish(exchange, routingKey, props, body);
    }
}
