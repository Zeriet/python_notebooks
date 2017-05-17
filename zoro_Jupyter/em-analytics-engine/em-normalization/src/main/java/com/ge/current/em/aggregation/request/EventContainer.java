package com.ge.current.em.aggregation.request;

import java.io.Serializable;
import java.util.Optional;
import com.ge.current.em.analytics.dto.JaceEvent;

/**
 * Created by 212582112 on 3/8/17.
 */
public class EventContainer implements Serializable {
    boolean valid;
    JaceEvent jaceEvent;
    String reasonPhrase;
    String rawEvent;

    public EventContainer(boolean status, JaceEvent jaceEvent, String reasonPhrase, String rawEvent) {
        this.valid = status;
        this.jaceEvent = jaceEvent;
        this.reasonPhrase = reasonPhrase;
        this.rawEvent = rawEvent;
    }

    public boolean isValid() {
        return valid;
    }

    public JaceEvent getJaceEvent() {
        return jaceEvent;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public String getRawEvent() {
        return rawEvent;
    }

    public static final class EventContainerBuilder {
        boolean valid;
        JaceEvent jaceEvent;
        String reasonPhrase;
        String rawEvent;

        private EventContainerBuilder() {
        }

        public static EventContainerBuilder anEventContainer() {
            return new EventContainerBuilder();
        }

        public EventContainerBuilder withInvalidEventStatus() {
            this.valid = false;
            return this;
        }

        public EventContainerBuilder withValidEventStatus() {
            this.valid = true;
            return this;
        }

        public EventContainerBuilder withEvent(JaceEvent event) {
            this.jaceEvent = event;
            return this;
        }

        public EventContainerBuilder withReasonPhrase(String reasonPhrase) {
            this.reasonPhrase = reasonPhrase;
            return this;
        }

        public EventContainerBuilder withRawEvent(String rawEvent) {
            this.rawEvent = rawEvent;
            return this;
        }

        public EventContainer build() {
            return new EventContainer(valid, jaceEvent, reasonPhrase, rawEvent);
        }
    }

    @Override
    public String toString() {
        return "EventContainer{" +
                "status=" + valid +
                ", jaceEvent=" + jaceEvent +
                ", reasonPhrase=" + reasonPhrase +
                ", rawEvent=" + rawEvent +
                '}';
    }
}
