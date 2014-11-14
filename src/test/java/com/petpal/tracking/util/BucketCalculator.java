package com.petpal.tracking.util;

import com.petpal.tracking.integration.TestTrackingData;
import com.petpal.tracking.integration.TestTrackingMetric;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Created by per on 11/3/14.
 */
public class BucketCalculator {

    private static Logger logger = Logger.getLogger(BucketCalculator.class);

    public static Calendar getCalendar(int year, int month, int date, int hour, int minute, int second, TimeZone timeZone) {
        return createCalendar(year, month, date, hour, minute, second, timeZone);
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

    public static void addDataPointForAllMetrics(TestTrackingData testTrackingData, Map<Long, Long> dataPoints) {
        for(TestTrackingMetric testTrackingMetric : TestTrackingMetric.getAllTrackingMetrics()) {
            testTrackingData.setDataForMetric(testTrackingMetric, dataPoints);
        }
    }

    public static TestTrackingData generateRandomTrackingData(
            String trackedEntityId,
            String trackingDeviceId,
            Calendar start,
            Calendar end) {

        int maxWalkingStepsPerMinute = 60;
        int maxRunningStepsPerMinute = 120;
        int maxSleepSecondsPerMinute = 50;
        int maxRestSecondsPerMinute = 50;

        TestTrackingData testTrackingData = new TestTrackingData();
        testTrackingData.setTrackedEntityId(trackedEntityId);
        testTrackingData.setTrackingDeviceId(trackingDeviceId);
        testTrackingData.setWalkingData(generateMinuteBucketRandomData(start, end, maxWalkingStepsPerMinute));
        testTrackingData.setRunningData(generateMinuteBucketRandomData(start, end, maxRunningStepsPerMinute));
        testTrackingData.setSleepingData(generateMinuteBucketRandomData(start, end, maxSleepSecondsPerMinute));
        testTrackingData.setRestingData(generateMinuteBucketRandomData(start, end, maxRestSecondsPerMinute));
        return testTrackingData;
    }

    public static TestTrackingData combineTrackingData(
            TestTrackingData testTrackingData1,
            TestTrackingData testTrackingData2) {

        if(!testTrackingData1.getTrackedEntityId().equals(testTrackingData2.getTrackedEntityId())) {
            throw new IllegalArgumentException("Tracked entity id must match");
        }

        if(!testTrackingData1.getTrackingDeviceId().equals(testTrackingData2.getTrackingDeviceId())) {
            throw new IllegalArgumentException("Tracked device id must match");
        }

        TestTrackingData combinedTestTrackingData = new TestTrackingData();
        combinedTestTrackingData.setTrackedEntityId(testTrackingData1.getTrackedEntityId());
        combinedTestTrackingData.setTrackingDeviceId(testTrackingData1.getTrackingDeviceId());

        for(TestTrackingMetric testTrackingMetric : TestTrackingMetric.getAllTrackingMetrics()) {
            Map<Long, Long> dataPoints = new TreeMap<Long, Long>();
            dataPoints.putAll(testTrackingData1.getDataForMetric(testTrackingMetric));
            dataPoints.putAll(testTrackingData2.getDataForMetric(testTrackingMetric));
            combinedTestTrackingData.setDataForMetric(testTrackingMetric, dataPoints);
        }

        return combinedTestTrackingData;
    }

    public static TreeMap<Long, Long> generateMinuteBucketRandomData(Calendar start, Calendar end, int maxRandomValue) {

        long generationStart = start.getTimeInMillis();
        long generationEnd = end.getTimeInMillis();

        if(generationEnd <= generationStart) {
            throw new IllegalArgumentException("Start time " + start + " must be before end time " + end);
        }

        long cursor = generationStart;

        TreeMap<Long, Long> timeData = new TreeMap<Long, Long>();

        while(cursor < generationEnd) {
            long bucketTimeStamp = cursor;
            long metricValue = randInt(0, maxRandomValue);
            timeData.put(bucketTimeStamp, metricValue);
            cursor = cursor + 60L*1000L;
        }

        logger.debug("generateMinuteBucketRandomData(): start = " + start + "end = " + end + ", # of datapoints = " + timeData.size());

        return timeData;
    }

    private static int randInt(int min, int max) {
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }

}
