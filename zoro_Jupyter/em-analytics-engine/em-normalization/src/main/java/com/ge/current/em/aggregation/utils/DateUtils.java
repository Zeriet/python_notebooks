package com.ge.current.em.aggregation.utils;

import java.text.SimpleDateFormat;
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

    public static Date convertToUTC(String timeString, String dateFormat) {
        utcCalender.setTime(getDateObj(timeString, dateFormat));
        return utcCalender.getTime();
    }
}
