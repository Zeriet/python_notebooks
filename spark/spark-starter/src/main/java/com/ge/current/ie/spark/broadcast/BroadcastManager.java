/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.broadcast;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.broadcast.Broadcast;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * A BroadcastManager is a central location of broadcasts throughout a
 * system. It implements a simple observer pattern for understanding when
 * a broadcast is updated.
 */
public class BroadcastManager implements Serializable {
    private Map<String, Broadcast> broadcasts = new ConcurrentHashMap<>();
    private transient MultiValueMap<String, Listener> listeners = new LinkedMultiValueMap<>();

    public <T> void put(String key, Broadcast<T> broadcast) {
        Broadcast<T> previous = broadcasts.put(key, broadcast);
        if (previous != null) {
            previous.unpersist(true);
            previous.destroy(true);
        }
        notifyListeners(key, broadcast);
    }

    private <T> void notifyListeners(String key, Broadcast<T> broadcast) {
        listeners.getOrDefault(key, Collections.emptyList())
            .forEach(l -> l.onBroadcastUpdate(broadcast));
    }

    public <T> T get(String key) {
        return getOrDefault(key, null);
    }

    public <T> T getOrDefault(String key, T def) {
        Broadcast<T> bc = broadcasts.get(key);
        if (bc != null) {
            return bc.getValue();
        }
        return def;
    }

    public <T> void addListener(String key, Listener<T> listener) {
        listeners.add(key, listener);
    }

    public <T> void removeListener(String key, Listener<T> listener) {
        listeners.remove(key, listener);
    }

    public interface Listener<T> {
        void onBroadcastUpdate(Broadcast<T> broadcast);
    }
}
