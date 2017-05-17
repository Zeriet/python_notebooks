package com.ge.current.em.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventQueryTimeUtil {
	
	static final Logger LOG = LoggerFactory.getLogger(EventQueryTimeUtil.class);
	
	public static String getTimeBucketFor15Mins(String time, String timeZone, String timeFrame) throws ParseException {

		String endTime = getEndDate(time,  timeZone,  timeFrame);

		String startTime = getStartDate(time,  timeZone,  timeFrame);

		return (startTime.equals(endTime)) ? startTime : (startTime + "','" + endTime);
	}

	public static String getStartTimeBucketForNormLog(String time, String timeZone, String timeFrame) throws ParseException {


		String startTime = getStartDate(time,  "UTC",  timeFrame) + getStartHourMin(time, timeFrame);

		return startTime;
	}
	
	public static String getEndTimeBucketForNormLog(String time, String timeZone, String timeFrame) throws ParseException {

		String endTime = getEndDate(time,  "UTC",  timeFrame) + getEndHourMin(time);

		

		return endTime;
	}
	
	public static String getStartDate(String time, String timeZone, String timeFrame) throws ParseException {
		LOG.info("time frame : yyyymmdd:******************** {}", timeFrame);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
		LOG.info("timezone: {}", timeZone);
		Date date = sdf.parse(time);
		LOG.info("getTimeBucket: {}", date.toString());
		Calendar today = Calendar.getInstance();

		today.clear(Calendar.DAY_OF_MONTH);
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);

		today.setTimeZone(TimeZone.getTimeZone(timeZone));
		today.setTime(date);
		LOG.info("yyyymmdd:******************** {}" , today.get(Calendar.YEAR) + "and"+ ((""+ today.get(Calendar.MONTH + 1)).length() < 2? "0" + (today.get(Calendar.MONTH)+1): (today.get(Calendar.MONTH)+1) +"" ) +""+ today.get(Calendar.DAY_OF_MONTH));

		today.add(Calendar.MINUTE, Integer.valueOf("-" + timeFrame));
		String mm = ((""+ today.get(Calendar.MONTH + 1)).length() < 2? "0" + (today.get(Calendar.MONTH)+1): (today.get(Calendar.MONTH)+1) +"" );
		String dd = ("" + today.get(Calendar.DAY_OF_MONTH)).length() < 2 ? "0" + (today.get(Calendar.DAY_OF_MONTH)): (today.get(Calendar.DAY_OF_MONTH)) +"" ;
		String startTime = today.get(Calendar.YEAR) + mm +""+ dd;
		return startTime;

	}

	public static String getEndDate(String time, String timeZone, String timeFrame) throws ParseException {
		LOG.info("time frame : yyyymmdd:******************** {}", timeFrame);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
		LOG.info("timezone: {}", timeZone);
		Date date = sdf.parse(time);
		LOG.info("getTimeBucket: {}", date.toString());
		Calendar today = Calendar.getInstance();

		today.clear(Calendar.DAY_OF_MONTH);
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);

		today.setTimeZone(TimeZone.getTimeZone(timeZone));
		today.setTime(date);
		LOG.info("yyyymmdd:******************** {}" , today.get(Calendar.YEAR) + "and"+ ((""+ today.get(Calendar.MONTH + 1)).length() < 2? "0" + (today.get(Calendar.MONTH)+1): (today.get(Calendar.MONTH)+1) +"" ) +""+ today.get(Calendar.DAY_OF_MONTH));
		String mm = ((""+ today.get(Calendar.MONTH + 1)).length() < 2? "0" + (today.get(Calendar.MONTH)+1): (today.get(Calendar.MONTH)+1) +"" );
		String dd = ("" + today.get(Calendar.DAY_OF_MONTH)).length() < 2 ? "0" + (today.get(Calendar.DAY_OF_MONTH)): (today.get(Calendar.DAY_OF_MONTH)) +"" ;
		String endTime = today.get(Calendar.YEAR) + mm +""+ dd;
		return endTime;
	}


	private static String getStartHourMin(String startTime, String timeFrame) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		Date date = sdf.parse(startTime);
		Calendar today = Calendar.getInstance();
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);
		today.setTime(date);
		today.add(Calendar.MINUTE, Integer.valueOf("-" + timeFrame));
		String hh = today.get(Calendar.HOUR_OF_DAY) < 10 ? "0" + today.get(Calendar.HOUR_OF_DAY) : today.get(Calendar.HOUR_OF_DAY) +"";
		String mm = today.get(Calendar.MINUTE) < 10 ? "0" + today.get(Calendar.MINUTE) : today.get(Calendar.MINUTE) +"";
		String hhmm = hh + mm;
		LOG.info("Endhhmm:******************** {}", hhmm);
		return hhmm;
	}

	private static String getEndHourMin(String startTime) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHHmm");
		Date date = sdf.parse(startTime);
		Calendar today = Calendar.getInstance();
		today.clear(Calendar.HOUR);
		today.clear(Calendar.MINUTE);
		today.clear(Calendar.SECOND);
		today.setTime(date);
		Date todayDate = today.getTime();
		String hh = today.get(Calendar.HOUR_OF_DAY) < 10 ? "0" + today.get(Calendar.HOUR_OF_DAY) : today.get(Calendar.HOUR_OF_DAY) +"";
		String mm = today.get(Calendar.MINUTE) < 10 ? "0" + today.get(Calendar.MINUTE) : today.get(Calendar.MINUTE) +"";
		String hhmm = hh + mm;
		LOG.info("Endhhmm:******************** {}", hhmm);
		return hhmm;
	}

}
