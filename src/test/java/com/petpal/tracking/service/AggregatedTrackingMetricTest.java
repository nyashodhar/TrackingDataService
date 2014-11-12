package com.petpal.tracking.service;

import org.junit.Assert;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by per on 11/12/14.
 */
public class AggregatedTrackingMetricTest {

    //
    // getAggregatedTrackingMetrics()
    //

    @Test(expected = IllegalArgumentException.class)
    public void getAggregatedTrackingMetrics_null_tracking_metric() {
        AggregatedTrackingMetric.getAggregatedTrackingMetrics(null, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAggregatedTrackingMetrics_invalid_time_unit() {
        AggregatedTrackingMetric.getAggregatedTrackingMetrics(TrackingMetric.WALKINGSTEPS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void getAggregatedTrackingMetrics_null_time_unit_walkingsteps() {
        List<AggregatedTrackingMetric> aggregatedTrackingMetrics =
            AggregatedTrackingMetric.getAggregatedTrackingMetrics(TrackingMetric.WALKINGSTEPS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.WALKINGSTEPS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTrackingMetrics_null_time_unit_runningsteps() {
        List<AggregatedTrackingMetric> aggregatedTrackingMetrics =
                AggregatedTrackingMetric.getAggregatedTrackingMetrics(TrackingMetric.RUNNINGSTEPS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.RUNNINGSTEPS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTrackingMetrics_null_time_unit_sleepingseconds() {
        List<AggregatedTrackingMetric> aggregatedTrackingMetrics =
                AggregatedTrackingMetric.getAggregatedTrackingMetrics(TrackingMetric.SLEEPINGSECONDS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.SLEEPINGSECONDS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTrackingMetrics_null_time_unit_restingseconds() {
        List<AggregatedTrackingMetric> aggregatedTrackingMetrics =
                AggregatedTrackingMetric.getAggregatedTrackingMetrics(TrackingMetric.RESTINGSECONDS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.RESTINGSECONDS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTrackingMetrics_valid_time_unit_walkingsteps() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.WALKINGSTEPS);
    }

    @Test
    public void getAggregatedTrackingMetrics_valid_time_unit_runningsteps() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.RUNNINGSTEPS);
    }

    @Test
    public void getAggregatedTrackingMetrics_valid_time_unit_sleeingseconds() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.SLEEPINGSECONDS);
    }

    @Test
    public void getAggregatedTrackingMetrics_valid_time_unit_restingseconds() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.RESTINGSECONDS);
    }

    private void checkAggregatedMetricForValidTimeUnits(TrackingMetric trackingMetric) {
        List<TimeUnit> validTimeUnits = getValidTimeUnits();
        for(TimeUnit timeUnit : validTimeUnits) {
            List<AggregatedTrackingMetric> aggregatedTrackingMetrics =
                    AggregatedTrackingMetric.getAggregatedTrackingMetrics(trackingMetric, timeUnit);
            Assert.assertEquals(1, aggregatedTrackingMetrics.size());
            Assert.assertTrue(aggregatedTrackingMetrics.contains(AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + timeUnit.toString())));
        }
    }

    private void assertAllValidTimeUnitsCovered(TrackingMetric trackingMetric, List<AggregatedTrackingMetric> aggregatedTrackingMetrics) {
        Assert.assertEquals(5, aggregatedTrackingMetrics.size());
        Assert.assertTrue(aggregatedTrackingMetrics.contains(AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.YEARS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.MONTHS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.WEEKS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.DAYS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.HOURS.toString())));
    }

    private List<TimeUnit> getValidTimeUnits() {
        List<TimeUnit> validTimeUnits = new ArrayList<TimeUnit>();
        validTimeUnits.add(TimeUnit.YEARS);
        validTimeUnits.add(TimeUnit.MONTHS);
        validTimeUnits.add(TimeUnit.WEEKS);
        validTimeUnits.add(TimeUnit.DAYS);
        validTimeUnits.add(TimeUnit.HOURS);
        return validTimeUnits;
    }
}
