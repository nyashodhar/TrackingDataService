package com.petpal.tracking.service.timeseries;

import com.petpal.tracking.service.timeseries.TimeSeriesMetric;
import com.petpal.tracking.web.controllers.AggregationLevel;
import com.petpal.tracking.web.controllers.TrackingMetric;
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
    // getAggregatedTimeSeriesMetric()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetAggregatedTimeSeriesMetric_tracking_metric_null() {
        TimeSeriesMetric.getAggregatedTimeSeriesMetric(null, AggregationLevel.YEARS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAggregatedTimeSeriesMetric_aggregation_level_null() {
        TimeSeriesMetric.getAggregatedTimeSeriesMetric(TrackingMetric.WALKINGSTEPS, null);
    }

    @Test
    public void getAggregatedTimeSeriesMetric_valid_aggregation_level_walkingsteps() {
        checkTimeSeriesMetricForAllAggregationLevels(TrackingMetric.WALKINGSTEPS);
    }

    @Test
    public void getAggregatedTimeSeriesMetric_valid_aggregation_level_runningsteps() {
        checkTimeSeriesMetricForAllAggregationLevels(TrackingMetric.RUNNINGSTEPS);
    }

    @Test
    public void getAggregatedTimeSeriesMetric_valid_aggregation_level_sleepingseconds() {
        checkTimeSeriesMetricForAllAggregationLevels(TrackingMetric.SLEEPINGSECONDS);
    }

    @Test
    public void getAggregatedTimeSeriesMetric_valid_aggregation_level_restingseconds() {
        checkTimeSeriesMetricForAllAggregationLevels(TrackingMetric.RESTINGSECONDS);
    }

    //
    // getAggregatedTimeSeriesMetrics()
    //

    @Test(expected = IllegalArgumentException.class)
    public void getAggregatedTimeSeriesMetrics_null_tracking_metric() {
        TimeSeriesMetric.getAggregatedTimeSeriesMetrics(null);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_walkingsteps() {
        List<TimeSeriesMetric> timeSeriesMetrics = TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.WALKINGSTEPS);
        assertAllValidTimeUnitsCovered(TrackingMetric.WALKINGSTEPS, timeSeriesMetrics);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_runningsteps() {
        List<TimeSeriesMetric> timeSeriesMetrics = TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.RUNNINGSTEPS);
        assertAllValidTimeUnitsCovered(TrackingMetric.RUNNINGSTEPS, timeSeriesMetrics);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_sleepingseconds() {
        List<TimeSeriesMetric> timeSeriesMetrics = TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.SLEEPINGSECONDS);
        assertAllValidTimeUnitsCovered(TrackingMetric.SLEEPINGSECONDS, timeSeriesMetrics);
    }

    @Test
    public void getAggregatedTimeSeriesMetrics_restingseconds() {
        List<TimeSeriesMetric> timeSeriesMetrics = TimeSeriesMetric.getAggregatedTimeSeriesMetrics(TrackingMetric.RESTINGSECONDS);
        assertAllValidTimeUnitsCovered(TrackingMetric.RESTINGSECONDS, timeSeriesMetrics);
    }


    private void checkTimeSeriesMetricForAllAggregationLevels(TrackingMetric trackingMetric) {
        for(AggregationLevel aggregationLevel : AggregationLevel.getAllAggregationLevels()) {
            TimeSeriesMetric timeSeriesMetric = TimeSeriesMetric.getAggregatedTimeSeriesMetric(trackingMetric, aggregationLevel);
            Assert.assertEquals(TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + aggregationLevel.toString().toUpperCase()), timeSeriesMetric);
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
}
