package com.ge.current.em.analytics.common;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {

    private static Calendar utcCalender = Calendar.getInstance();
    static {
        utcCalender.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static String getTimeBucket(long eventTimestamp)
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(eventTimestamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        String timeString = sdf.format(cal.getTime());
        int minute = Integer.parseInt(timeString.substring(10));
        // calculate the 15 mins bucket
        int minBucket = (minute/15)*15;
        timeString = timeString.substring(0, 10) + minBucket;

        if (minBucket == 0) {
            timeString = timeString +"0";
        }
        return timeString;
        //return timeString.substring(0,10);
    }

    public static Date getDateFromTimeStringTimeZone(String timeString, String timeformat, String timeZone) throws Exception {
        return Date.from(getZonedDateTimeFromTimeStringTimeZone(timeString, timeformat, timeZone).toInstant());
    }

    public static ZonedDateTime getZonedDateTimeFromTimeStringTimeZone(String timeString, String timeformat, String timeZone) throws Exception {
        SimpleDateFormat dateFormater = new SimpleDateFormat(timeformat);
        dateFormater.setTimeZone(TimeZone.getTimeZone(timeZone));
        Date parsedDate = dateFormater.parse(timeString);

        return ZonedDateTime.ofInstant(parsedDate.toInstant(), ZoneId.of(timeZone));
    }

    public static Date getDateObj(String timeString, String dateFormat)
    {
    	SimpleDateFormat dateFormater = new SimpleDateFormat(dateFormat);
    	
        if (timeString == null) return null;
        Date resultDate = null;
        try {
            resultDate = dateFormater.parse(timeString);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultDate;
    }

    public static Date convertToUTC(String timeString, String dateFormat) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.parse(timeString);
    }

    public static ZonedDateTime parseTimeWithOffset(String timeString, String dateFormat, String timeZoneId) {
        return parseTime(timeString, dateFormat).withZoneSameInstant(ZoneId.of(timeZoneId));
    }

    public static ZonedDateTime parseTime(String timeString, String dateFormat) {
        return ZonedDateTime.parse(timeString, DateTimeFormatter.ofPattern(dateFormat));
    }

    public static String convertUTCStringToLocalDateString(String utcTime, String localTimezone, String localDateFormat) {
        return ZonedDateTime.ofInstant(Instant.parse(utcTime), ZoneId.of(localTimezone)).format(DateTimeFormatter.ofPattern(localDateFormat));
    }

    public static String convertUTCStringToLocalTimeString(String utcTime,
                                                           String utcTimeFormat,
                                                           String localTimezone,
                                                           String localDateFormat)
                         throws Exception {

        return DateUtils.convertUTCDateToLocalString(DateUtils.convertToUTC(utcTime, utcTimeFormat), localTimezone, localDateFormat);
    }

    public static String convertUTCDateToLocalString(Date utcDate, String timeZone, String localDateFormat) {
        return utcDate.toInstant().atZone(ZoneId.of(timeZone)).format(DateTimeFormatter.ofPattern(localDateFormat));
    }
}
