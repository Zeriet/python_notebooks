/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.streaming.receivers;

import static com.ge.current.ie.spark.model.message.Message.MessageBuilder.aMessage;

import com.rabbitmq.client.Channel;
import org.apache.spark.storage.StorageLevel;

import com.ge.current.ie.spark.model.message.Message;

public class StandardRabbitMqReceiver extends AbstractRabbitMqReceiver<Message> {
    public StandardRabbitMqReceiver(StorageLevel storageLevel) {
        super(storageLevel);
    }

    @Override
    public void onMessage(org.springframework.amqp.core.Message message, Channel channel) throws Exception {
        Message standardMessage = mapAmqpMessageToStandard(message);
        store(standardMessage);
    }

    private Message mapAmqpMessageToStandard(org.springframework.amqp.core.Message message) {
        return aMessage()
                .withTopic(message.getMessageProperties().getReceivedRoutingKey())
                .withBody(message.getBody())
                .withHeaders(message.getMessageProperties().getHeaders())
                .build();
    }
}
