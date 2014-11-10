package com.petpal.tracking.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by per on 11/10/14.
 */
public class BucketAggregationUtilTest {

    // Class under test
    private BucketAggregationUtil bucketAggregationUtil;

    @Before
    public void setup() {
        bucketAggregationUtil = new BucketAggregationUtil();
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
