package com.petpal.tracking.util;

import com.petpal.tracking.integration.TestTrackingDataUpload;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.Assert;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
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
        logger.info("printUTCForCalendar: calendar.getTime() = " + calendar.getTime() + ", utc millis = " + calendar.getTimeInMillis());
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

            //
            // INSANE: Weeks don't follow years! For example Dec 28 of 2014 can belong
            // to week 1! Apparently because week 1 is in some locales defined as the
            // day to which Jan 1 belongs. Therefore, do full manual reset here to get
            // the returned value correct when dealing with weeks.
            //

            output.setTimeInMillis(input.getTimeInMillis());
            output.set(Calendar.DAY_OF_WEEK, 1);
            output.set(Calendar.HOUR_OF_DAY, 0);
            output.set(Calendar.MINUTE, 0);
            output.set(Calendar.SECOND, 0);
            output.set(Calendar.MILLISECOND, 0);

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


    /**
     * Calculate the end time of a bucket given its start time and bucket size
     * @param bucketStart
     * @param bucketSize
     * @param timeZone
     * @return the end time of a bucket
     */
    public static long getBucketEndTime(Long bucketStart, TimeUnit bucketSize, TimeZone timeZone) {

        Assert.notNull(bucketSize, "Bucket size not specified");
        Assert.notNull(bucketStart, "Bucket start not specified");

        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeZone(timeZone);
        cal.setTimeInMillis(bucketStart);

        int fieldToStepForward;

        if(bucketSize == TimeUnit.YEARS) {
            fieldToStepForward = Calendar.YEAR;
        } else if(bucketSize == TimeUnit.MONTHS) {
            fieldToStepForward = Calendar.MONTH;
        } else if(bucketSize == TimeUnit.WEEKS) {
            fieldToStepForward = Calendar.WEEK_OF_YEAR;
        } else if(bucketSize == TimeUnit.DAYS) {
            fieldToStepForward = Calendar.DATE;
        } else if(bucketSize == TimeUnit.HOURS) {
            fieldToStepForward = Calendar.HOUR;
        } else if(bucketSize == TimeUnit.MINUTES) {
            fieldToStepForward = Calendar.MINUTE;
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit for bucketsize: " + bucketSize);
        }

        cal.add(fieldToStepForward, 1);
        return (cal.getTimeInMillis() - 1L);
    }

    public static void addDataPointForAllLongMetrics(TestTrackingDataUpload testTrackingData, TreeMap<Long, Long> dataPoints) {

        Map<String, TreeMap<Long, Long>> longMetricsAndDataPoints =
                new HashMap<String, TreeMap<Long, Long>>(dataPoints.size());

        for(String longMetricName : TrackingMetricConfigUtil.getAllLongTypeMetrics()) {
            longMetricsAndDataPoints.put(longMetricName, dataPoints);
        }

        testTrackingData.setLongMetrics(longMetricsAndDataPoints);

    }

    public static TestTrackingDataUpload generateRandomLongTrackingData(
            Calendar start,
            Calendar end) {

        int maxWalkingStepsPerMinute = 60;
        int maxRunningStepsPerMinute = 120;
        int maxSleepSecondsPerMinute = 50;
        int maxRestSecondsPerMinute = 50;

        Map<String, TreeMap<Long, Long>> longMetricsAndDataPoints =
                new HashMap<String, TreeMap<Long, Long>>(4);

        longMetricsAndDataPoints.put(TrackingMetricConfigUtil.METRIC_WALKING_STEPS,
                generateMinuteBucketRandomData(start, end, maxWalkingStepsPerMinute));
        longMetricsAndDataPoints.put(TrackingMetricConfigUtil.METRIC_RUNNING_STEPS,
                generateMinuteBucketRandomData(start, end, maxRunningStepsPerMinute));
        longMetricsAndDataPoints.put(TrackingMetricConfigUtil.METRIC_SLEEPING_SECONDS,
                generateMinuteBucketRandomData(start, end, maxSleepSecondsPerMinute));
        longMetricsAndDataPoints.put(TrackingMetricConfigUtil.METRIC_RESTING_SECONDS,
                generateMinuteBucketRandomData(start, end, maxRestSecondsPerMinute));

        TestTrackingDataUpload testTrackingData = new TestTrackingDataUpload();
        testTrackingData.setLongMetrics(longMetricsAndDataPoints);
        return testTrackingData;
    }

    public static TestTrackingDataUpload combineLongTrackingData(
            TestTrackingDataUpload testTrackingData1,
            TestTrackingDataUpload testTrackingData2) {

        TestTrackingDataUpload combinedTestTrackingData = new TestTrackingDataUpload();

        Map<String, TreeMap<Long, Long>> longMetricsAndDataPointsForCombined =
                new HashMap<String, TreeMap<Long, Long>>();

        for(String longMetricName : testTrackingData1.getLongMetrics().keySet()) {
            TreeMap<Long, Long> dataPoints = new TreeMap<Long, Long>();
            dataPoints.putAll(testTrackingData1.getLongMetrics().get(longMetricName));
            dataPoints.putAll(testTrackingData2.getLongMetrics().get(longMetricName));
            longMetricsAndDataPointsForCombined.put(longMetricName, dataPoints);
        }

        combinedTestTrackingData.setLongMetrics(longMetricsAndDataPointsForCombined);

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
