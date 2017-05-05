package com.ge.current.ie.spark.broadcast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.spark.broadcast.Broadcast;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.ge.current.ie.spark.testing.rule.EmbeddedSpark;

public class BroadcastManagerTest {

    @Rule
    public EmbeddedSpark embeddedSpark = new EmbeddedSpark();

    private BroadcastManager broadcastManager;

    @Before
    public void setUp() throws Exception {
        broadcastManager = new BroadcastManager();
    }

    @Test
    public void itShouldSaveTheBroadcast() throws Exception {
        Broadcast<Integer> broadcast = embeddedSpark.broadcast(1);
        String key = "theInt";

        broadcastManager.put(key, broadcast);

        Integer other = broadcastManager.get(key);
        assertEquals(broadcast.getValue(), other);
    }

    @Test
    public void itShouldUpdateTheBroadcast() throws Exception {
        Broadcast<Integer> broadcast = embeddedSpark.broadcast(1);
        String key = "theInt";

        broadcastManager.put(key, broadcast);

        Broadcast<Integer> next = embeddedSpark.broadcast(2);
        broadcastManager.put(key, next);

        assertFalse(broadcast.isValid());

        Integer other = broadcastManager.get(key);
        assertEquals(next.getValue(), other);
    }

    @Test
    public void itShouldNotifyListeners() throws Exception {
        BroadcastManager.Listener listener = mock(BroadcastManager.Listener.class);
        String key = "theInt";

        broadcastManager.addListener(key, listener);

        Broadcast<Integer> broadcast = embeddedSpark.broadcast(1);
        broadcastManager.put(key, broadcast);

        verify(listener).onBroadcastUpdate(broadcast);
    }
}