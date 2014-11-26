package com.petpal.tracking.web.validators.util;

import com.petpal.tracking.util.BucketCalculator;
import com.petpal.tracking.web.errors.InvalidControllerArgumentException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by per on 11/24/14.
 */
public class AggregatedQueryUtilTest {

    private TimeZone timeZonePST;

    @Before
    public void setup() {
        timeZonePST = TimeZone.getTimeZone("PST");
    }

    //
    // calculateUTCBegin()
    //

    @Test
    public void testCalculateUTCBegin_years() {

        Long utcStart = AggregatedQueryUtil.calculateUTCBegin(2014, null, null, null, null, TimeUnit.YEARS, timeZonePST);

        Calendar expected = Calendar.getInstance();
        expected.clear();
        expected.setTimeZone(timeZonePST);
        expected.set(2014, 1, 1, 0, 0, 0);

        Assert.assertEquals(expected.getTimeInMillis(), utcStart.longValue());
    }

    @Test
    public void testCalculateUTCBegin_months() {

        Long utcStart = AggregatedQueryUtil.calculateUTCBegin(2014, 3, null, null, null, TimeUnit.MONTHS, timeZonePST);

        Calendar expected = Calendar.getInstance();
        expected.clear();
        expected.setTimeZone(timeZonePST);
        expected.set(2014, 3, 1, 0, 0, 0);

        Assert.assertEquals(expected.getTimeInMillis(), utcStart.longValue());
    }

    @Test
    public void testCalculateUTCBegin_weeks() {

        Long utcStart = AggregatedQueryUtil.calculateUTCBegin(2014, null, 7, null, null, TimeUnit.WEEKS, timeZonePST);

        Calendar expected = Calendar.getInstance();
        expected.clear();
        expected.setTimeZone(timeZonePST);
        expected.set(2014, 1, 1, 0, 0, 0);
        expected.add(Calendar.WEEK_OF_YEAR, 7);

        Assert.assertEquals(expected.getTimeInMillis(), utcStart.longValue());
    }

    @Test
    public void testCalculateUTCBegin_days() {

        Long utcStart = AggregatedQueryUtil.calculateUTCBegin(2014, 3, null, 8, null, TimeUnit.DAYS, timeZonePST);

        Calendar expected = Calendar.getInstance();
        expected.clear();
        expected.setTimeZone(timeZonePST);
        expected.set(2014, 3, 8, 0, 0, 0);

        Assert.assertEquals(expected.getTimeInMillis(), utcStart.longValue());
    }

    @Test
    public void testCalculateUTCBegin_hours() {

        Long utcStart = AggregatedQueryUtil.calculateUTCBegin(2014, 3, null, 8, 5, TimeUnit.HOURS, timeZonePST);

        Calendar expected = Calendar.getInstance();
        expected.clear();
        expected.setTimeZone(timeZonePST);
        expected.set(2014, 3, 8, 5, 0, 0);

        Assert.assertEquals(expected.getTimeInMillis(), utcStart.longValue());
    }

    //
    // calculateUTCEnd()
    //

    @Test(expected = InvalidControllerArgumentException.class)
    public void testCalculateUTCEnd_utc_begin_is_null() {
        AggregatedQueryUtil.calculateUTCEnd(null, TimeUnit.MONTHS, 1, timeZonePST);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testCalculateUTCEnd_bucket_size_is_null() {
        AggregatedQueryUtil.calculateUTCEnd(123L, null, 1, timeZonePST);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testCalculateUTCEnd_bucket_size_is_millis() {
        AggregatedQueryUtil.calculateUTCEnd(123L, TimeUnit.MILLISECONDS, 1, timeZonePST);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testCalculateUTCEnd_bucket_size_is_seconds() {
        AggregatedQueryUtil.calculateUTCEnd(123L, TimeUnit.SECONDS, 1, timeZonePST);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testCalculateUTCEnd_bucket_size_is_minutes() {
        AggregatedQueryUtil.calculateUTCEnd(123L, TimeUnit.MINUTES, 1, timeZonePST);
    }

    @Test
    public void testCalculateUTCEnd_buckets_to_fetch_is_null() {
        Long utcEnd = AggregatedQueryUtil.calculateUTCEnd(123L, TimeUnit.MONTHS, null, timeZonePST);
        Assert.assertEquals(null, utcEnd);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testCalculateUTCEnd_buckets_to_fetch_is_negative() {
        AggregatedQueryUtil.calculateUTCEnd(123L, TimeUnit.MONTHS, -1, timeZonePST);
    }

    @Test
    public void testCalculateUTCEnd_end_is_in_future_default_to_null() {
        Long utcEnd = AggregatedQueryUtil.calculateUTCEnd(System.currentTimeMillis(), TimeUnit.MONTHS, 1, timeZonePST);
        Assert.assertEquals(null, utcEnd);
    }

    @Test
    public void testCalculateUTCEnd_end_time_accurate() {

        // Make the start time at least 1 hour into past
        Long utcStart = System.currentTimeMillis() - 60L*60L*1000L - 3000L;
        Long utcEnd = AggregatedQueryUtil.calculateUTCEnd(utcStart, TimeUnit.HOURS, 1, timeZonePST);
        Assert.assertEquals(BucketCalculator.getBucketEndTime(utcStart, TimeUnit.HOURS, timeZonePST), utcEnd.longValue());
    }


    //
    // validateAggregatedQueryParams()
    //

    @Test
    public void testValidateAggregatedQueryParams_valid() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, null, null, TimeUnit.YEARS);
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, null, TimeUnit.MONTHS);
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 2, null, null, TimeUnit.WEEKS);
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, null, TimeUnit.DAYS);
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, 2, TimeUnit.HOURS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_year_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(null, null, null, null, null, TimeUnit.YEARS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_year_too_far_back() {
        AggregatedQueryUtil.validateAggregatedQueryParams(null, null, null, null, null, TimeUnit.YEARS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_month_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, -1, null, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_month_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 12, null, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_week_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 0, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_week_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 53, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_day_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 0, null, TimeUnit.DAYS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_day_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 32, null, TimeUnit.DAYS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_hour_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, -1, TimeUnit.HOURS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_start_hour_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, 24, TimeUnit.HOURS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_month_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, null, TimeUnit.YEARS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_week_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 2, null, null, TimeUnit.YEARS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_day_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, 2, null, TimeUnit.YEARS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, null, 2, TimeUnit.YEARS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_month_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_week_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_day_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, null, TimeUnit.MONTHS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, 2, TimeUnit.MONTHS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_week_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_month_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_day_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 2, 2, null, TimeUnit.WEEKS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 2, null, 2, TimeUnit.WEEKS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_month_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, 2, null, TimeUnit.DAYS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_weeks_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, 2, null, TimeUnit.DAYS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_day_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, null, TimeUnit.DAYS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, 2, TimeUnit.DAYS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_month_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, 2, 2, TimeUnit.HOURS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_weeks_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, 2, 2, TimeUnit.HOURS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_day_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, 2, TimeUnit.HOURS);
    }

    @Test(expected = InvalidControllerArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_hour_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, null, TimeUnit.HOURS);
    }
}
