package com.ge.current.em.analytics.common;

import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * Created by 212582112 on 1/9/17.
 */
public class DateUtilsTest {
    @Test
    public void testConvertUTCDateToLocalString() {
        Date utcDate = Date.from(ZonedDateTime.parse("2017-01-05T04:45:00Z").toInstant());
        assertEquals("201701042045", DateUtils.convertUTCDateToLocalString(utcDate, "America/Los_Angeles", "yyyyMMddHHmm"));
        assertEquals("201701042245", DateUtils.convertUTCDateToLocalString(utcDate, "America/Chicago", "yyyyMMddHHmm"));
        assertEquals("201701042345", DateUtils.convertUTCDateToLocalString(utcDate, "America/New_York", "yyyyMMddHHmm"));
    }

    @Test
    public void testHourlyLocalTimestamp() throws Exception {
        Date utcDate = DateUtils.convertToUTC("201701160400".substring(0, 10), "yyyyMMddHH");
        assertEquals("2017011520", DateUtils.convertUTCDateToLocalString(utcDate, "America/Los_Angeles", "yyyyMMddHH"));
    }

    @Test
    public void testGetDateFromTimeStringTimeZone() throws Exception {
        System.out.println(DateUtils.getDateFromTimeStringTimeZone("2017011623", "yyyyMMddHH", "America/Los_Angeles"));
    }

    @Test
    public void testParseTime() throws Exception {
        String sampleDate = "2017-01-23T18:15:08.499-08:00";
        try {
            assertEquals("America/Los_Angeles", DateUtils.parseTimeWithOffset(sampleDate, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "America/Los_Angeles").getZone().getId());
        }catch(DateTimeParseException dtpe) {
            System.out.println("Failed to parse from: " + sampleDate.substring(dtpe.getErrorIndex()));
            throw dtpe;
        }
    }

    @Test
    public void testTimeWithZoneName() {
        String sample = "2017-01-26T15:49:03.533-08:00 America/Los_Angeles";
        try {
            assertEquals("America/Los_Angeles", DateUtils.parseTime(sample, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX VV").getZone().getId());
        }catch (DateTimeParseException e) {
            System.out.println("Error at: " + sample.substring(e.getErrorIndex()));
            throw e;
        }
    }

    @Test
    public void testWacTimeParsing() {
        String sample = "2017-03-14T22:14:02.026Z";

    }
}