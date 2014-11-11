package com.petpal.tracking.util;

import org.kairosdb.client.builder.TimeUnit;

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


    public static Calendar getBucketStartForCalendar(Calendar input, TimeUnit bucketSize) {

        //
        // To determine the bucket interval start, reset all the time components that are of
        // less significance than the desired bucket size time unit
        //

        Calendar output = Calendar.getInstance();
        output.clear();
        output.setTimeZone(input.getTimeZone());

        if (bucketSize == TimeUnit.YEARS) {
            output.set(Calendar.YEAR, input.get(Calendar.YEAR));
        } else if (bucketSize == TimeUnit.MONTHS) {
            output.set(Calendar.YEAR, input.get(Calendar.YEAR));
            output.set(Calendar.MONTH, input.get(Calendar.MONTH));
        } else if (bucketSize == TimeUnit.WEEKS) {
            output.set(Calendar.YEAR, input.get(Calendar.YEAR));
            output.set(Calendar.WEEK_OF_YEAR, input.get(Calendar.WEEK_OF_YEAR));
            output.set(Calendar.DAY_OF_WEEK, 1);
        } else if (bucketSize == TimeUnit.DAYS) {
            output.set(Calendar.YEAR, input.get(Calendar.YEAR));
            output.set(Calendar.MONTH, input.get(Calendar.MONTH));
            output.set(Calendar.DAY_OF_MONTH, input.get(Calendar.DAY_OF_MONTH));
        } else if (bucketSize == TimeUnit.HOURS) {
            output.set(Calendar.YEAR, input.get(Calendar.YEAR));
            output.set(Calendar.MONTH, input.get(Calendar.MONTH));
            output.set(Calendar.DAY_OF_MONTH, input.get(Calendar.DAY_OF_MONTH));
            output.set(Calendar.HOUR_OF_DAY, input.get(Calendar.HOUR_OF_DAY));
        } else if (bucketSize == TimeUnit.MINUTES) {
            throw new IllegalArgumentException("TimeUnit MINUTES not supported.");
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit " + bucketSize);
        }

        return output;
    }

}
