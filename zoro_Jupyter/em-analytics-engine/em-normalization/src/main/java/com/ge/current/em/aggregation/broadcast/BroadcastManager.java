package com.ge.current.em.aggregation.broadcast;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * Created by 212582112 on 3/6/17.
 */
public class BroadcastManager implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(BroadcastManager.class);
    private JavaStreamingContext jsc;

    public BroadcastManager(JavaStreamingContext jsc) {
        this.jsc = jsc;
    }

    public <T> Broadcast<T> refreshBroadcast(Broadcast<T> currentBroadcast,
                                             T defaultValue,
                                             Function0<T> refreshFunction) throws Exception {
        Broadcast<T> oldBroadcast = currentBroadcast;

        T value = refreshFunction.call();
        if(value == null) {
            value = defaultValue;
        }

        currentBroadcast = jsc.sparkContext().broadcast(value);
        if(oldBroadcast != null) oldBroadcast.unpersist();

        System.out.println("Broadcast value: " + currentBroadcast.value());
        if(currentBroadcast == null) {
            System.out.println(">>> Value was not successfully BCASTed");
        }
        return currentBroadcast;
    }

    public static final class BroadcastManagerBuilder {
        private JavaStreamingContext jsc;

        private BroadcastManagerBuilder() {
        }

        public static BroadcastManagerBuilder aBroadcastManager() {
            return new BroadcastManagerBuilder();
        }

        public BroadcastManagerBuilder withJavaStreamingContext(JavaStreamingContext jsc) {
            this.jsc = jsc;
            return this;
        }

        public BroadcastManager build() {
            return new BroadcastManager(jsc);
        }
    }
}
