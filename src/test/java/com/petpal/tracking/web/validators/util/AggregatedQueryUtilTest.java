package com.petpal.tracking.web.validators.util;

import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

/**
 * Created by per on 11/24/14.
 */
public class AggregatedQueryUtilTest {

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

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_year_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(null, null, null, null, null, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_year_too_far_back() {
        AggregatedQueryUtil.validateAggregatedQueryParams(null, null, null, null, null, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_month_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, -1, null, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_month_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 12, null, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_week_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 0, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_week_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 53, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_day_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 0, null, TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_day_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 32, null, TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_hour_too_low() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, -1, TimeUnit.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_start_hour_too_high() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, 24, TimeUnit.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_month_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, null, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_week_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 2, null, null, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_day_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, 2, null, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_years_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, null, 2, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_month_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_week_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, null, null, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_day_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, null, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_months_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, 2, TimeUnit.MONTHS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_week_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_month_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, null, null, TimeUnit.WEEKS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_day_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 2, 2, null, TimeUnit.WEEKS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_weeks_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, 2, null, 2, TimeUnit.WEEKS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_month_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, 2, null, TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_weeks_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, 2, null, TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_day_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, null, TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_days_start_hour_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, 2, TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_month_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, null, null, 2, 2, TimeUnit.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_weeks_not_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, 2, 2, 2, TimeUnit.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_day_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, null, 2, TimeUnit.HOURS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateAggregatedQueryParams_hours_start_hour_is_null() {
        AggregatedQueryUtil.validateAggregatedQueryParams(2014, 2, null, 2, null, TimeUnit.HOURS);
    }
}
