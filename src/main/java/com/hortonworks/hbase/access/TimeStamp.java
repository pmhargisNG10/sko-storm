package com.hortonworks.hbase.access;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

public class TimeStamp {

	private static final Logger logger = Logger.getLogger(TimeStamp.class);

	public static String getTimeStamp() {

		try {

			return (new SimpleDateFormat("yyyy:MM:dd HH:mm:ss:SSS").format(Calendar.getInstance().getTime()));
		}
		catch (Exception e) {

			String timeStamp = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss:SSS").format(Calendar.getInstance().getTime());

			System.err.println("[" + timeStamp + "] " + "TimeStamp::getTimeStamp()::" +  e.getMessage());

			e.printStackTrace(System.err);

			logger.error("[" + timeStamp + "] " + "TimeStamp::getTimeStamp()::" +  e.getMessage(), e);

			return "";
		}
	}
}
