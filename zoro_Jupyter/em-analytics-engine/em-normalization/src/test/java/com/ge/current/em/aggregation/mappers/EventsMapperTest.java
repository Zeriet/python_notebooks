package com.ge.current.em.aggregation.mappers;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by 212582112 on 2/22/17.
 */
public class EventsMapperTest {
    EventsMapper eventsMapper;

    @Before
    public void setup() {
        eventsMapper = new EventsMapper();
    }

    @Test
    public void testEventLogGeneration() {

    }

    @After
    public void cleanup() {
        eventsMapper = null;
    }
}
