package com.petpal.tracking.service.metrics;

import com.petpal.tracking.service.TrackingMetric;
import org.junit.Assert;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by per on 11/12/14.
 */
public class TimeSeriesMetricTest {

    //
    // getRawMetric()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetRawMetric_null_tracking_metric() {
        TimeSeriesMetric.getRawMetric(null);
    }

    @Test
    public void testGetRawMetric_all_metrics() {

        List<TrackingMetric> allMetrics = new ArrayList<TrackingMetric>();
        allMetrics.add(TrackingMetric.WALKINGSTEPS);
        allMetrics.add(TrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TrackingMetric.RESTINGSECONDS);

        for(TrackingMetric trackingMetric : allMetrics) {
            TimeSeriesMetric rawMetric = TimeSeriesMetric.getRawMetric(trackingMetric);
            Assert.assertEquals(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_RAW"), rawMetric);
        }
    }


    //
    // getAggregatedTimeSeriesMetrics()
    //

    @Test(expected = IllegalArgumentException.class)
    public void getAggregatedTimeSeriesMetrics_null_tracking_metric() {
        TimeSeriesMetric.getAggregatedTimeSeriesMetrics(null, TimeUnit.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAggregatedTimeSeriesMetrics_invalid_time_unit() {
        TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.WALKINGSTEPS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_null_time_unit_walkingsteps() {
        List<TimeSeriesMetric> aggregatedTrackingMetrics =
                TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.WALKINGSTEPS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.WALKINGSTEPS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_null_time_unit_runningsteps() {
        List<TimeSeriesMetric> aggregatedTrackingMetrics =
                TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.RUNNINGSTEPS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.RUNNINGSTEPS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_null_time_unit_sleepingseconds() {
        List<TimeSeriesMetric> aggregatedTrackingMetrics =
                TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.SLEEPINGSECONDS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.SLEEPINGSECONDS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_null_time_unit_restingseconds() {
        List<TimeSeriesMetric> aggregatedTrackingMetrics =
                TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.RESTINGSECONDS, null);
        assertAllValidTimeUnitsCovered(TrackingMetric.RESTINGSECONDS, aggregatedTrackingMetrics);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_valid_time_unit_walkingsteps() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.WALKINGSTEPS);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_valid_time_unit_runningsteps() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.RUNNINGSTEPS);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_valid_time_unit_sleeingseconds() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.SLEEPINGSECONDS);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_valid_time_unit_restingseconds() {
        checkAggregatedMetricForValidTimeUnits(TrackingMetric.RESTINGSECONDS);
    }

    private void checkAggregatedMetricForValidTimeUnits(TrackingMetric trackingMetric) {
        List<TimeUnit> validTimeUnits = getValidTimeUnits();
        for(TimeUnit timeUnit : validTimeUnits) {
            List<TimeSeriesMetric> aggregatedTrackingMetrics =
                    TimeSeriesMetric.getAggregatedTimeSeriesMetrics(trackingMetric, timeUnit);
            Assert.assertEquals(1, aggregatedTrackingMetrics.size());
            Assert.assertTrue(aggregatedTrackingMetrics.contains(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + timeUnit.toString())));
        }
    }

    private void assertAllValidTimeUnitsCovered(TrackingMetric trackingMetric, List<TimeSeriesMetric> aggregatedTrackingMetrics) {
        Assert.assertEquals(5, aggregatedTrackingMetrics.size());
        Assert.assertTrue(aggregatedTrackingMetrics.contains(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.YEARS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.MONTHS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.WEEKS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.DAYS.toString())));
        Assert.assertTrue(aggregatedTrackingMetrics.contains(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + TimeUnit.HOURS.toString())));
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
