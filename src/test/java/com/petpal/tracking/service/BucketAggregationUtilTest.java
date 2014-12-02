package com.petpal.tracking.service;

import com.petpal.tracking.util.BucketCalculator;
import com.petpal.tracking.web.controllers.AggregationLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.CollectionUtils;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Created by per on 11/10/14.
 */
public class BucketAggregationUtilTest {

    // Class under test
    private BucketAggregationUtil bucketAggregationUtil;

    // Some class variables
    private TimeZone timeZonePST;
    private TimeZone timeZoneUTC;

    @Before
    public void setup() {
        bucketAggregationUtil = new BucketAggregationUtil();
        timeZonePST = TimeZone.getTimeZone("PST");
        timeZoneUTC = TimeZone.getTimeZone("UTC");
    }

    //
    // applyFortyEightHourShift()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testApplyFortyEightHourShift_null_input() {
        bucketAggregationUtil.applyFortyEightHourShift(null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testApplyFortyEightHourShift_empty_input() {
        bucketAggregationUtil.applyFortyEightHourShift(new HashMap<Long, Long>(), true);
    }

    @Test
    public void testApplyFortyEightHourShift_forward() {

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();
        dataPoints.put(System.currentTimeMillis() - 10000L, 5L);
        dataPoints.put(System.currentTimeMillis() - 20000L, 5L);

        Map<Long, Long> shiftedDataPoints = bucketAggregationUtil.applyFortyEightHourShift(dataPoints, true);

        Assert.assertEquals(2, shiftedDataPoints.size());

        for(long timestamp : dataPoints.keySet()) {
            long expectedNewTimeStamp = timestamp + 48L*60L*60L*1000L;
            Assert.assertEquals(shiftedDataPoints.get(expectedNewTimeStamp),  dataPoints.get(timestamp));
        }
    }


    @Test
    public void testApplyFortyEightHourShift_backward() {

        Map<Long, Long> dataPoints = new TreeMap<Long, Long>();
        dataPoints.put(System.currentTimeMillis() - 10000L, 5L);
        dataPoints.put(System.currentTimeMillis() - 20000L, 5L);

        Map<Long, Long> shiftedDataPoints = bucketAggregationUtil.applyFortyEightHourShift(dataPoints, false);

        Assert.assertEquals(2, shiftedDataPoints.size());

        for(long timestamp : dataPoints.keySet()) {
            long expectedNewTimeStamp = timestamp - 48L*60L*60L*1000L;
            Assert.assertEquals(shiftedDataPoints.get(expectedNewTimeStamp),  dataPoints.get(timestamp));
        }
    }


    //
    // mergeExistingDataPointsIntoNew()
    //

    @Test
    public void testMergeExistingDataPointsIntoNew_both_args_null() {
        Map<Long, Long> updatedDataPoints = bucketAggregationUtil.mergeExistingDataPointsIntoNew(null, null);
        Assert.assertTrue(CollectionUtils.isEmpty(updatedDataPoints));
    }

    @Test
    public void testMergeExistingDataPointsIntoNew_new_data_null_old_data_empty() {
        Map<Long, Long> updatedDataPoints = bucketAggregationUtil.mergeExistingDataPointsIntoNew(null, new HashMap<Long, Long>());
        Assert.assertTrue(CollectionUtils.isEmpty(updatedDataPoints));
    }

    @Test
    public void testMergeExistingDataPointsIntoNew_new_data_empty_old_data_null() {
        Map<Long, Long> updatedDataPoints = bucketAggregationUtil.mergeExistingDataPointsIntoNew(new HashMap<Long, Long>(), null);
        Assert.assertTrue(CollectionUtils.isEmpty(updatedDataPoints));
    }

    @Test
    public void testMergeExistingDataPointsIntoNew_new_data_empty_old_data_empty() {
        Map<Long, Long> updatedDataPoints = bucketAggregationUtil.mergeExistingDataPointsIntoNew(new HashMap<Long, Long>(), new HashMap<Long, Long>());
        Assert.assertTrue(CollectionUtils.isEmpty(updatedDataPoints));
    }

    @Test
    public void testMergeExistingDataPointsIntoNew_new_data_not_empty_old_data_empty() {

        Map<Long, Long> newDataPoints = new HashMap<Long, Long>();
        newDataPoints.put(1L, 333L);
        newDataPoints.put(2L, 444L);

        Map<Long, Long> updatedDataPoints = bucketAggregationUtil.mergeExistingDataPointsIntoNew(newDataPoints, new HashMap<Long, Long>());
        Assert.assertEquals(2, updatedDataPoints.size());
        Assert.assertEquals(newDataPoints.get(1L), updatedDataPoints.get(1L));
        Assert.assertEquals(newDataPoints.get(2L), updatedDataPoints.get(2L));
    }

    @Test
    public void testMergeExistingDataPointsIntoNew_new_data_not_empty_and_old_data_not_empty() {

        Map<Long, Long> newDataPoints = new HashMap<Long, Long>();
        newDataPoints.put(1L, 333L);
        newDataPoints.put(2L, 444L);

        Map<Long, Long> existingDataPoints = new HashMap<Long, Long>();
        existingDataPoints.put(2L, 1L);
        existingDataPoints.put(7L, 888L);

        Map<Long, Long> updatedDataPoints = bucketAggregationUtil.mergeExistingDataPointsIntoNew(newDataPoints, existingDataPoints);
        Assert.assertEquals(2, updatedDataPoints.size());
        Assert.assertEquals(newDataPoints.get(1L), updatedDataPoints.get(1L));

        Long expectedSum = newDataPoints.get(2L) + existingDataPoints.get(2L);
        Assert.assertEquals(expectedSum, updatedDataPoints.get(2L));
    }

    //
    // aggregateIntoBucketsForTimeZone()
    //

    @Test
    public void aggregateIntoBucketsForTimeZone_null_aggregated_data() {
        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                null, timeZonePST, AggregationLevel.MONTHS);
        Assert.assertNull(aggregatedData);
    }

    @Test
    public void aggregateIntoBucketsForTimeZone_empty_aggregated_data() {
        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                new TreeMap<Long, Long>(), timeZonePST, AggregationLevel.MONTHS);
        Assert.assertNull(aggregatedData);
    }

    @Test(expected = IllegalArgumentException.class)
    public void aggregateIntoBucketsForTimeZone_timezone_missing() {
        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(1L, 1L);
        bucketAggregationUtil.aggregateIntoBucketsForTimeZone(unaggregatedData, null, AggregationLevel.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void aggregateIntoBucketsForTimeZone_bucketsize_missing() {
        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(1L, 1L);
        bucketAggregationUtil.aggregateIntoBucketsForTimeZone(unaggregatedData, timeZonePST, null);
    }

    @Test
    public void aggregateIntoBucketsForTimeZone_years_with_gap() {

        Calendar cal1 = BucketCalculator.getCalendar(2012, Calendar.MAY, 1, 0, 0, 0, timeZonePST);
        Calendar cal2 = BucketCalculator.getCalendar(2012, Calendar.MAY, 2, 0, 0, 0, timeZonePST);
        Calendar cal3 = BucketCalculator.getCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePST);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePST, AggregationLevel.YEARS);

        long bucket1Start = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.YEARS).getTimeInMillis();
        long bucket2Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.YEARS).getTimeInMillis();

        Assert.assertEquals(2, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket2Start).longValue());
    }

    @Test
    public void aggregateIntoBucketsForTimeZone_months_with_gap() {

        Calendar cal1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 0, timeZonePST);
        Calendar cal2 = BucketCalculator.getCalendar(2014, Calendar.MAY, 2, 0, 0, 2, timeZonePST);
        Calendar cal3 = BucketCalculator.getCalendar(2014, Calendar.JULY, 1, 0, 0, 0, timeZonePST);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePST, AggregationLevel.MONTHS);

        long bucket1Start = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.MONTHS).getTimeInMillis();
        long bucket2Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.MONTHS).getTimeInMillis();

        Assert.assertEquals(2, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket2Start).longValue());
    }

    @Test
    public void aggregateIntoBucketsForTimeZone_weeks_with_gap() {

        //
        // Setup:
        // DP1: 3 weeks ago - 5 steps
        // DP2: 2 weeks - 1 day ago - 6 steps
        // DP3: Just now - 3 steps
        //

        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeZone(timeZonePST);
        cal1.add(Calendar.WEEK_OF_YEAR, -3);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeZone(timeZonePST);
        cal2.add(Calendar.WEEK_OF_YEAR, -2);

        Calendar cal3 = Calendar.getInstance();
        cal3.setTimeZone(timeZonePST);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePST, AggregationLevel.WEEKS);
        //
        // The aggregated data should be in 3 buckets
        // Bucket 1 should start at midnight 4 weeks ago and have value 5
        // Bucket 2 should start at midnight 3 weeks ago and have value 6
        // Bucket 3 should start at beginning of current week at midnight and have value 3
        //

        long bucket1Start = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.WEEKS).getTimeInMillis();
        long bucket2Start = BucketCalculator.getBucketStartForCalendar(cal2, TimeUnit.WEEKS).getTimeInMillis();
        long bucket3Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.WEEKS).getTimeInMillis();

        Assert.assertEquals(3, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(5L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(6L, aggregatedData.get(bucket2Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket3Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket3Start).longValue());
    }


    @Test
    public void aggregateIntoBucketsForTimeZone_days_with_gap() {

        Calendar cal1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 1, timeZonePST);
        Calendar cal2 = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 0, 0, 2, timeZonePST);
        Calendar cal3 = BucketCalculator.getCalendar(2014, Calendar.MAY, 3, 0, 0, 0, timeZonePST);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePST, AggregationLevel.DAYS);

        long bucket1Start = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.DAYS).getTimeInMillis();
        long bucket2Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.DAYS).getTimeInMillis();

        Assert.assertEquals(2, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket2Start).longValue());
    }


    @Test
    public void aggregateIntoBucketsForTimeZone_hours_with_gap() {

        Calendar cal1 = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 1, 2, 0, timeZonePST);
        Calendar cal2 = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 1, 3, 0, timeZonePST);
        Calendar cal3 = BucketCalculator.getCalendar(2014, Calendar.MAY, 1, 3, 0, 0, timeZonePST);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePST, AggregationLevel.HOURS);

        long bucket1Start = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.HOURS).getTimeInMillis();
        long bucket2Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.HOURS).getTimeInMillis();

        Assert.assertEquals(2, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket2Start).longValue());
    }

    //
    // getShiftedTimeStamp()
    //

    @Test
    public void test_getShiftedTimeStamp_year_PST() {

        long now = System.currentTimeMillis();

        Calendar nowPST = Calendar.getInstance();
        nowPST.setTimeZone(timeZonePST);
        nowPST.setTimeInMillis(now);

        long timestamp = bucketAggregationUtil.getShiftedTimeStamp(now, timeZonePST, timeZoneUTC, AggregationLevel.YEARS);

        // Expect the determined bucket to start on Jan 1, 00:00:00:000 relative to UTC

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(timeZoneUTC);
        bucketStartCal.setTimeInMillis(timestamp);

        Assert.assertEquals(nowPST.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(Calendar.JANUARY, bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(1, bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }


    @Test
    public void test_getShiftedTimeStamp_month_PST() {

        long now = System.currentTimeMillis();

        Calendar nowPST = Calendar.getInstance();
        nowPST.setTimeZone(timeZonePST);
        nowPST.setTimeInMillis(now);

        long initialBucketStart = bucketAggregationUtil.getShiftedTimeStamp(now, timeZonePST, timeZoneUTC, AggregationLevel.MONTHS);

        // Expect the determined bucket to start the first of the month, 00:00:00:000 relative to UTC timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(timeZoneUTC);
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(nowPST.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(nowPST.get(Calendar.MONTH), bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(1, bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_getShiftedTimeStamp_week_PST() {

        long now = System.currentTimeMillis();

        Calendar nowPST = Calendar.getInstance();
        nowPST.setTimeZone(timeZonePST);
        nowPST.setTimeInMillis(now);

        long initialBucketStart = bucketAggregationUtil.getShiftedTimeStamp(now, timeZonePST, timeZoneUTC, AggregationLevel.WEEKS);

        // Expect the determined bucket to start the first day of week of given timestamp, 00:00:00:000 relative to UTC

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(timeZoneUTC);
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(nowPST.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(nowPST.get(Calendar.WEEK_OF_YEAR), bucketStartCal.get(Calendar.WEEK_OF_YEAR));
        Assert.assertEquals(1, bucketStartCal.get(Calendar.DAY_OF_WEEK));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_getShiftedTimeStamp_day_PST() {

        long now = System.currentTimeMillis();

        Calendar nowPST = Calendar.getInstance();
        nowPST.setTimeZone(timeZonePST);
        nowPST.setTimeInMillis(now);

        long initialBucketStart = bucketAggregationUtil.getShiftedTimeStamp(now, timeZonePST, timeZoneUTC, AggregationLevel.DAYS);

        // Expect the determined bucket to start 00:00:00:000 on the same day as the given timestamp relative to UTC

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(timeZoneUTC);
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(nowPST.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(nowPST.get(Calendar.MONTH), bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(nowPST.get(Calendar.DAY_OF_MONTH), bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_getShiftedTimeStamp_hour_PST() {

        long now = System.currentTimeMillis();

        Calendar nowPST = Calendar.getInstance();
        nowPST.setTimeZone(timeZonePST);
        nowPST.setTimeInMillis(now);

        long initialBucketStart = bucketAggregationUtil.getShiftedTimeStamp(now, timeZonePST, timeZoneUTC, AggregationLevel.HOURS);

        // Expect the determined bucket to start XX:00:00:000 where XX is the hour in the given timestamp relative to our timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(timeZoneUTC);
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(nowPST.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(nowPST.get(Calendar.MONTH), bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(nowPST.get(Calendar.DAY_OF_MONTH), bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(nowPST.get(Calendar.HOUR_OF_DAY), bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }


    @Test(expected = IllegalArgumentException.class)
    public void test_getShiftedTimeStamp_missing_timestamp() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        bucketAggregationUtil.getShiftedTimeStamp(null, now.getTimeZone(), timeZoneUTC, AggregationLevel.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getShiftedTimeStamp_missing_timezone1() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        bucketAggregationUtil.getShiftedTimeStamp(now.getTimeInMillis(), null, timeZoneUTC, AggregationLevel.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getShiftedTimeStamp_missing_timezone2() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        bucketAggregationUtil.getShiftedTimeStamp(now.getTimeInMillis(), timeZonePST, null, AggregationLevel.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_getShiftedTimeStamp_missing_timeunit() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        bucketAggregationUtil.getShiftedTimeStamp(now.getTimeInMillis(), now.getTimeZone(), timeZoneUTC, null);
    }



    //
    // determineInitialBucket()
    //

    @Test
    public void test_determineInitialBucket_year_PST() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), AggregationLevel.YEARS);

        // Expect the determined bucket to start on Jan 1, 00:00:00:000 relative to our timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(now.getTimeZone());
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(now.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(Calendar.JANUARY, bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(1, bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_determineInitialBucket_month_PST() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), AggregationLevel.MONTHS);

        // Expect the determined bucket to start the first of the month, 00:00:00:000 relative to our timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(now.getTimeZone());
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(now.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(now.get(Calendar.MONTH), bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(1, bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_determineInitialBucket_week_PST() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), AggregationLevel.WEEKS);

        // Expect the determined bucket to start the first day of week of given timestamp, 00:00:00:000 relative to our timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(now.getTimeZone());
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(now.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(now.get(Calendar.WEEK_OF_YEAR), bucketStartCal.get(Calendar.WEEK_OF_YEAR));
        Assert.assertEquals(1, bucketStartCal.get(Calendar.DAY_OF_WEEK));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_determineInitialBucket_day_PST() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), AggregationLevel.DAYS);

        // Expect the determined bucket to start 00:00:00:000 on the same day as the given timestamp relative to our timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(now.getTimeZone());
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(now.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(now.get(Calendar.MONTH), bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(now.get(Calendar.DAY_OF_MONTH), bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_determineInitialBucket_hour_PST() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), AggregationLevel.HOURS);

        // Expect the determined bucket to start XX:00:00:000 where XX is the hour in the given timestamp relative to our timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(now.getTimeZone());
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(now.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(now.get(Calendar.MONTH), bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(now.get(Calendar.DAY_OF_MONTH), bucketStartCal.get(Calendar.DAY_OF_MONTH));
        Assert.assertEquals(now.get(Calendar.HOUR_OF_DAY), bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_missing_timestamp() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        bucketAggregationUtil.determineInitialBucket(null, now.getTimeZone(), AggregationLevel.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_missing_timezone() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), null, AggregationLevel.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_missing_aggregation_level() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), null);
    }
}
