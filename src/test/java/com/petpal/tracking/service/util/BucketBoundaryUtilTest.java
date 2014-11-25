package com.petpal.tracking.service.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by per on 11/24/14.
 */
public class BucketBoundaryUtilTest {

    // Some class variables
    private TimeZone timeZonePST;

    @Before
    public void setup() {
        timeZonePST = TimeZone.getTimeZone("PST");
    }

    //
    // getBucketEndTime()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_start_missing() {
        BucketBoundaryUtil.getBucketEndTime(null, TimeUnit.MONTHS, timeZonePST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_size_missing() {
        BucketBoundaryUtil.getBucketEndTime(1L, null, timeZonePST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_size_millis_not_allowed() {
        BucketBoundaryUtil.getBucketEndTime(1L, TimeUnit.MILLISECONDS, timeZonePST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBucketEndTime_bucket_size_seconds_not_allowed() {
        BucketBoundaryUtil.getBucketEndTime(1L, TimeUnit.SECONDS, timeZonePST);
    }

    @Test
    public void testGetBucketEndTime_years() {

        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = BucketBoundaryUtil.getBucketEndTime(bucketStart, TimeUnit.YEARS, timeZonePST);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(timeZonePST);
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.YEAR, 1);

        Assert.assertEquals((calExpected.getTimeInMillis() - 1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_months() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = BucketBoundaryUtil.getBucketEndTime(bucketStart, TimeUnit.MONTHS, timeZonePST);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(timeZonePST);
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.MONTH, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_weeks() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = BucketBoundaryUtil.getBucketEndTime(bucketStart, TimeUnit.WEEKS, timeZonePST);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(timeZonePST);
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.WEEK_OF_YEAR, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_days() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = BucketBoundaryUtil.getBucketEndTime(bucketStart, TimeUnit.DAYS, timeZonePST);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(timeZonePST);
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.DATE, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_hours() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = BucketBoundaryUtil.getBucketEndTime(bucketStart, TimeUnit.HOURS, timeZonePST);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(timeZonePST);
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.HOUR, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }

    @Test
    public void testGetBucketEndTime_minutes() {
        Calendar now = Calendar.getInstance();
        now.setTimeZone(timeZonePST);
        long bucketStart = now.getTimeInMillis();
        long bucketEndTime = BucketBoundaryUtil.getBucketEndTime(bucketStart, TimeUnit.MINUTES, timeZonePST);

        Calendar calExpected = Calendar.getInstance();
        calExpected.clear();
        calExpected.setTimeZone(timeZonePST);
        calExpected.setTimeInMillis(bucketStart);
        calExpected.add(Calendar.MINUTE, 1);

        Assert.assertEquals((calExpected.getTimeInMillis()-1L), bucketEndTime);
    }
}
