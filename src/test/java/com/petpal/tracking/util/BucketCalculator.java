package com.petpal.tracking.util;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Created by per on 11/3/14.
 */
public class BucketCalculator {

    public static long getUTCMillis(int year, int month, int date, int hour, int minute, int second, TimeZone timeZone) {
        Calendar calendar = createCalendar(year, month, date, hour, minute, second, timeZone);
        return calendar.getTimeInMillis();
    }

    public static void printUTCForCalendar(int year, int month, int date, int hour, int minute, int second, TimeZone timeZone) {
        Calendar calendar = createCalendar(year, month, date, hour, minute, second, timeZone);
        System.out.println("printUTCForCalendar: calendar.getTime() = " + calendar.getTime() + ", utc millis = " + calendar.getTimeInMillis());
    }

    private static Calendar createCalendar(int year, int month, int date, int hour, int minute, int second, TimeZone timeZone) {
        Calendar calendar = new GregorianCalendar();
        calendar.clear();
        calendar.setTimeZone(timeZone);
        calendar.set(year, month, date, hour, minute, second);
        return calendar;
    }
}
