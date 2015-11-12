package com.cassandra.spring_cassandra.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	public static final String DEFAULT_DATE_FORMAT_PATTERN_FULL = "yyyy-MM-dd HH:mm:ss";
	
	public static Integer transDateToInt(Date date){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		StringBuilder sb = new StringBuilder();
		sb.append(calendar.get(Calendar.YEAR))
		.append(calendar.get(Calendar.MONTH) + 1 < 10 ? "0" + calendar.get(Calendar.MONTH) + 1 : calendar.get(Calendar.MONTH) + 1)
		.append(calendar.get(Calendar.DAY_OF_MONTH) > 10 ? calendar.get(Calendar.DAY_OF_MONTH) : "0" + calendar.get(Calendar.DAY_OF_MONTH));
		return Integer.valueOf(sb.toString());
	}
	
	/**
     * 将dateTimeString按照默认格式yyyy-MM-dd HH:mm:ss转换成Date
     *
     * @param dateTimeString
     * @param pattern
     * @return
     */
    public static Date getDateByString(String dateTimeString) {
    	DateFormat df = new SimpleDateFormat(DEFAULT_DATE_FORMAT_PATTERN_FULL);
    	try {
			return df.parse(dateTimeString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return null;
    }
    
	
	public static void main(String[] args) {
//		System.err.println(DateUtil.transDateToInt(new Date()));
		
		System.err.println(getDateByString("2015-10-16 00:00:00"));
	}
}
