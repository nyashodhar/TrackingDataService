package com.petpal.tracking.service;

import com.petpal.tracking.util.BucketCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Created by per on 11/10/14.
 */
public class BucketAggregationUtilTest {

    // Class under test
    private BucketAggregationUtil bucketAggregationUtil;

    // Some class variables
    private TimeZone timeZonePDT;

    @Before
    public void setup() {
        bucketAggregationUtil = new BucketAggregationUtil();
        timeZonePDT = TimeZone.getTimeZone("America/Los_Angeles");
    }

    //
    // aggregateIntoBucketsForTimeZone()
    //

    @Test
    public void aggregateIntoBucketsForTimeZone_null_aggregated_data() {
        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                null, TimeZone.getTimeZone("America/Los_Angeles"), TimeUnit.MONTHS);
        Assert.assertNull(aggregatedData);
    }

    @Test
    public void aggregateIntoBucketsForTimeZone_empty_aggregated_data() {
        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                new TreeMap<Long, Long>(), TimeZone.getTimeZone("America/Los_Angeles"), TimeUnit.MONTHS);
        Assert.assertNull(aggregatedData);
    }

    @Test(expected = IllegalArgumentException.class)
    public void aggregateIntoBucketsForTimeZone_timezone_missing() {
        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(1L, 1L);
        bucketAggregationUtil.aggregateIntoBucketsForTimeZone(unaggregatedData, null, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void aggregateIntoBucketsForTimeZone_bucketsize_missing() {
        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(1L, 1L);
        bucketAggregationUtil.aggregateIntoBucketsForTimeZone(unaggregatedData, TimeZone.getTimeZone("America/Los_Angeles"), null);
    }


    @Test(expected = IllegalArgumentException.class)
    public void aggregateIntoBucketsForTimeZone_minutes_not_allowed() {
        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(1L, 5L);
        bucketAggregationUtil.aggregateIntoBucketsForTimeZone(unaggregatedData, timeZonePDT, TimeUnit.MINUTES);
    }


    @Test
    public void aggregateIntoBucketsForTimeZone_years_with_gap() {

        //
        // Setup:
        // DP1: 2 1/2 years ago - 5 steps
        // DP2: 2 1/2 years - 1 days ago - 6 steps
        // DP3: Half a year ago - 3 steps
        //

        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeZone(timeZonePDT);
        cal1.add(Calendar.MONTH, -30);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeZone(timeZonePDT);
        cal2.add(Calendar.DAY_OF_YEAR, 1);
        cal2.add(Calendar.MONTH, -30);

        Calendar cal3 = Calendar.getInstance();
        cal3.setTimeZone(timeZonePDT);
        cal3.add(Calendar.MONTH, -6);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePDT, TimeUnit.YEARS);

        //
        // The aggregated data should be in 3 buckets
        // Bucket 1 should start at midnight 3 years ago and have value 11
        // Bucket 2 should start at midnight 2 years ago and have value 0
        // Bucket 3 should start at midnight 1 year ago and have value 3
        //

        Calendar tmp = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.YEARS);
        long bucket1Start = tmp.getTimeInMillis();
        tmp.add(Calendar.YEAR, 1);
        long bucket2Start = tmp.getTimeInMillis();
        long bucket3Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.YEARS).getTimeInMillis();

        Assert.assertEquals(3, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(0L, aggregatedData.get(bucket2Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket3Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket3Start).longValue());
    }

    @Test
    public void aggregateIntoBucketsForTimeZone_months_with_gap() {

        //
        // Setup:
        // DP1: 2 1/2 months ago - 5 steps
        // DP2: 2 1/2 months - 1 days ago - 6 steps
        // DP3: Half a month ago - 3 steps
        //

        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeZone(timeZonePDT);
        cal1.add(Calendar.DATE, -75);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeZone(timeZonePDT);
        cal2.add(Calendar.DATE, 1);
        cal2.add(Calendar.DATE, -75);

        Calendar cal3 = Calendar.getInstance();
        cal3.setTimeZone(timeZonePDT);
        cal3.add(Calendar.DATE, -15);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePDT, TimeUnit.MONTHS);

        //
        // The aggregated data should be in 3 buckets
        // Bucket 1 should start at midnight 3 months ago and have value 11
        // Bucket 2 should start at midnight 2 months ago and have value 0
        // Bucket 3 should start at midnight 1 month ago and have value 3
        //

        Calendar tmp = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.MONTHS);
        long bucket1Start = tmp.getTimeInMillis();
        tmp.add(Calendar.MONTH, 1);
        long bucket2Start = tmp.getTimeInMillis();
        long bucket3Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.MONTHS).getTimeInMillis();

        Assert.assertEquals(3, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(0L, aggregatedData.get(bucket2Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket3Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket3Start).longValue());
    }

    @Test
    public void aggregateIntoBucketsForTimeZone_weeks_with_gap() {

        //
        // Setup:
        // DP1: 2 1/2 weeks ago - 5 steps
        // DP2: 2 1/2 weeks - 1 day ago - 6 steps
        // DP3: Half a week ago - 3 steps
        //

        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeZone(timeZonePDT);
        cal1.add(Calendar.DATE, -17);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeZone(timeZonePDT);
        cal2.add(Calendar.DATE, 1);
        cal2.add(Calendar.DATE, -17);

        Calendar cal3 = Calendar.getInstance();
        cal3.setTimeZone(timeZonePDT);
        cal3.add(Calendar.DATE, -3);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePDT, TimeUnit.WEEKS);

        //
        // The aggregated data should be in 3 buckets
        // Bucket 1 should start at midnight 3 weeks ago and have value 11
        // Bucket 2 should start at midnight 2 weeks ago and have value 0
        // Bucket 3 should start at midnight 1 weeks ago and have value 3
        //

        Calendar tmp = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.WEEKS);
        long bucket1Start = tmp.getTimeInMillis();
        tmp.add(Calendar.WEEK_OF_YEAR, 1);
        long bucket2Start = tmp.getTimeInMillis();
        long bucket3Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.WEEKS).getTimeInMillis();

        Assert.assertEquals(3, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(0L, aggregatedData.get(bucket2Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket3Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket3Start).longValue());
    }


    @Test
    public void aggregateIntoBucketsForTimeZone_days_with_gap() {

        //
        // Setup:
        // DP1: 2 1/2 days ago - 5 steps
        // DP2: 2 1/2 days - 1 hour ago - 6 steps
        // DP3: Half a day ago - 3 steps
        //

        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeZone(timeZonePDT);
        cal1.add(Calendar.HOUR, -60);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeZone(timeZonePDT);
        cal2.add(Calendar.HOUR, 1);
        cal2.add(Calendar.HOUR, -60);

        Calendar cal3 = Calendar.getInstance();
        cal3.setTimeZone(timeZonePDT);
        cal3.add(Calendar.HOUR, -12);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePDT, TimeUnit.DAYS);

        //
        // The aggregated data should be in 3 buckets
        // Bucket 1 should start at midnight 3 days ago and have value 11
        // Bucket 2 should start at midnight 2 days ago and have value 0
        // Bucket 3 should start at midnight 1 days ago and have value 3
        //

        Calendar tmp = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.DAYS);
        long bucket1Start = tmp.getTimeInMillis();
        tmp.add(Calendar.DATE, 1);
        long bucket2Start = tmp.getTimeInMillis();
        long bucket3Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.DAYS).getTimeInMillis();

        Assert.assertEquals(3, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(0L, aggregatedData.get(bucket2Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket3Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket3Start).longValue());
    }


    @Test
    public void aggregateIntoBucketsForTimeZone_hours_with_gap() {

        //
        // Setup:
        // DP1: 2 1/2 hours ago - 5 steps
        // DP2: 2 1/2 hours - 1 minute ago - 6 steps
        // DP3: Half an hour ago - 3 steps
        //

        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeZone(timeZonePDT);
        cal1.add(Calendar.MINUTE, -150);

        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeZone(timeZonePDT);
        cal2.add(Calendar.MINUTE, 1);
        cal2.add(Calendar.MINUTE, -150);

        Calendar cal3 = Calendar.getInstance();
        cal3.setTimeZone(timeZonePDT);
        cal3.add(Calendar.MINUTE, -30);

        TreeMap<Long, Long> unaggregatedData = new TreeMap<Long, Long>();
        unaggregatedData.put(cal1.getTimeInMillis(), 5L);
        unaggregatedData.put(cal2.getTimeInMillis(), 6L);
        unaggregatedData.put(cal3.getTimeInMillis(), 3L);

        TreeMap<Long, Long> aggregatedData = bucketAggregationUtil.aggregateIntoBucketsForTimeZone(
                unaggregatedData, timeZonePDT, TimeUnit.HOURS);

        //
        // The aggregated data should be in 3 buckets
        // Bucket 1 should start at midnight 3 hours ago and have value 11
        // Bucket 2 should start at midnight 2 hours ago and have value 0
        // Bucket 3 should start at midnight 1 hours ago and have value 3
        //

        Calendar tmp = BucketCalculator.getBucketStartForCalendar(cal1, TimeUnit.HOURS);
        long bucket1Start = tmp.getTimeInMillis();
        tmp.add(Calendar.HOUR, 1);
        long bucket2Start = tmp.getTimeInMillis();
        long bucket3Start = BucketCalculator.getBucketStartForCalendar(cal3, TimeUnit.HOURS).getTimeInMillis();

        Assert.assertEquals(3, aggregatedData.size());
        Assert.assertNotNull(aggregatedData.get(bucket1Start));
        Assert.assertEquals(11L, aggregatedData.get(bucket1Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket2Start));
        Assert.assertEquals(0L, aggregatedData.get(bucket2Start).longValue());
        Assert.assertNotNull(aggregatedData.get(bucket3Start));
        Assert.assertEquals(3L, aggregatedData.get(bucket3Start).longValue());
    }


    //
    // getBucketEndTime()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_start_missing() {
        bucketAggregationUtil.getBucketEndTime(null, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_size_missing() {
        bucketAggregationUtil.getBucketEndTime(1L, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_size_millis_not_allowed() {
        bucketAggregationUtil.getBucketEndTime(1L, TimeUnit.MILLISECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_size_seconds_not_allowed() {
        bucketAggregationUtil.getBucketEndTime(1L, TimeUnit.SECONDS);
    }

    @Test
    public void testGetBucketEndTime_years() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = bucketAggregationUtil.getBucketEndTime(bucketStart, TimeUnit.YEARS);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.YEAR, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_months() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = bucketAggregationUtil.getBucketEndTime(bucketStart, TimeUnit.MONTHS);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.MONTH, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_weeks() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = bucketAggregationUtil.getBucketEndTime(bucketStart, TimeUnit.WEEKS);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.WEEK_OF_YEAR, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_days() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = bucketAggregationUtil.getBucketEndTime(bucketStart, TimeUnit.DAYS);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.DATE, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_hours() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = bucketAggregationUtil.getBucketEndTime(bucketStart, TimeUnit.HOURS);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.HOUR, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_minutes() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = bucketAggregationUtil.getBucketEndTime(bucketStart, TimeUnit.MINUTES);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.MINUTE, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    //
    // determineInitialBucket()
    //

    @Test
    public void test_determineInitialBucket_year_PDT() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.YEARS);

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
    public void test_determineInitialBucket_month_PDT() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.MONTHS);

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
    public void test_determineInitialBucket_week_PDT() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.WEEKS);

        // Expect the determined bucket to start the first day of week of given timestamp, 00:00:00:000 relative to our timezone

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(now.getTimeZone());
        bucketStartCal.setTimeInMillis(initialBucketStart);

        Assert.assertEquals(now.get(Calendar.YEAR), bucketStartCal.get(Calendar.YEAR));
        Assert.assertEquals(now.get(Calendar.MONTH), bucketStartCal.get(Calendar.MONTH));
        Assert.assertEquals(now.get(Calendar.WEEK_OF_YEAR), bucketStartCal.get(Calendar.WEEK_OF_YEAR));
        Assert.assertEquals(1, bucketStartCal.get(Calendar.DAY_OF_WEEK));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.HOUR_OF_DAY));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MINUTE));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.SECOND));
        Assert.assertEquals(0, bucketStartCal.get(Calendar.MILLISECOND));
    }

    @Test
    public void test_determineInitialBucket_day_PDT() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.DAYS);

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
    public void test_determineInitialBucket_hour_PDT() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        long initialBucketStart = bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.HOURS);

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
    public void test_determineInitialBucket_minute_not_allowed() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.MINUTES);
    }


    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_seconds_not_allowed() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.SECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_millis_not_allowed() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), TimeUnit.MILLISECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_missing_timestamp() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        bucketAggregationUtil.determineInitialBucket(null, now.getTimeZone(), TimeUnit.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_missing_timezone() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), null, TimeUnit.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_determineInitialBucket_missing_timeunit() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        bucketAggregationUtil.determineInitialBucket(now.getTimeInMillis(), now.getTimeZone(), null);
    }
}
