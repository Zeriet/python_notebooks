package com.ge.current.ie.spark.streaming.receivers;

import static org.junit.Assert.assertEquals;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.ge.current.ie.spark.model.message.Message;
import com.ge.current.ie.spark.testing.rule.EmbeddedAmqpBroker;
import com.ge.current.ie.spark.testing.rule.EmbeddedSparkStreaming;

public class StandardRabbitMqReceiverTest {

    private static final String QUEUE = "ie.spark.test";

    @Rule
    public EmbeddedAmqpBroker broker = new EmbeddedAmqpBroker();

    @Rule
    public EmbeddedSparkStreaming sparkStreaming = new EmbeddedSparkStreaming();

    private StandardRabbitMqReceiver standardRabbitMqReceiver;

    @Before
    public void setUp() throws Exception {
        standardRabbitMqReceiver = new StandardRabbitMqReceiver(StorageLevel.MEMORY_ONLY_SER_2());
        standardRabbitMqReceiver.setUri(broker.getUri());
        standardRabbitMqReceiver.addQueue(QUEUE);

        broker.queueDeclare(QUEUE);
    }

    @Test
    public void itShouldConsumeMessagesFromRabbitMq() throws Exception {
        Counter counter = new Counter();
        sparkStreaming.receiverStream(standardRabbitMqReceiver)
                .foreach(counter);

        broker.publish("", QUEUE, "Hello, world!".getBytes());

        sparkStreaming.start();
        sparkStreaming.awaitTermination(1000);

        assertEquals(1L, counter.getCount());
    }

    public class Counter implements Function<JavaRDD<Message>, Void> {

        private long count;

        @Override
        public Void call(JavaRDD<Message> v1) throws Exception {
            count += v1.count();
            return null;
        }

        public long getCount() {
            return count;
        }
    }
}