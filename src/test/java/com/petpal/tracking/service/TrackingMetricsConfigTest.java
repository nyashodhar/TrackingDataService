package com.petpal.tracking.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Properties;

/**
 * Created by per on 12/5/14.
 */
public class TrackingMetricsConfigTest {

    private static final String CONFIG_FILE = "tracking-metrics.properties";

    // Class under test
    private TrackingMetricsConfig trackingMetricsConfig;

    // Other
    Properties trackingMetricsProperties;


    @Before
    public void setup() throws Throwable {
        Resource resource = new ClassPathResource("/" + CONFIG_FILE);
        trackingMetricsProperties = PropertiesLoaderUtils.loadProperties(resource);
        trackingMetricsConfig = new TrackingMetricsConfig();
    }

    //
    // initialize()
    //

    @Test
    public void testInitialize() throws IOException {

        trackingMetricsConfig.initialize();
        Map<String, TrackingMetricsConfig.TrackingMetric> trackingMetricMap =
                trackingMetricsConfig.getAllMetrics();
        Assert.assertEquals(4, trackingMetricMap.size());
        Assert.assertTrue(trackingMetricMap.containsKey("WALKINGSTEPS"));
        Assert.assertTrue(trackingMetricMap.containsKey("RUNNINGSTEPS"));
        Assert.assertTrue(trackingMetricMap.containsKey("SLEEPINGSECONDS"));
        Assert.assertTrue(trackingMetricMap.containsKey("RESTINGSECONDS"));

        TrackingMetricsConfig.TrackingMetric walkingSteps = trackingMetricMap.get("WALKINGSTEPS");
        Assert.assertEquals(Long.class, walkingSteps.getDataType());
        Assert.assertEquals(TrackingMetricsConfig.Aggregation.SUM, walkingSteps.getAggregation());
        Assert.assertEquals(walkingSteps, trackingMetricsConfig.getTrackingMetric("WALKINGSTEPS"));

        TrackingMetricsConfig.TrackingMetric runningSteps = trackingMetricMap.get("RUNNINGSTEPS");
        Assert.assertEquals(Long.class, runningSteps.getDataType());
        Assert.assertEquals(TrackingMetricsConfig.Aggregation.SUM, runningSteps.getAggregation());
        Assert.assertEquals(runningSteps, trackingMetricsConfig.getTrackingMetric("RUNNINGSTEPS"));

        TrackingMetricsConfig.TrackingMetric sleepingSeconds = trackingMetricMap.get("SLEEPINGSECONDS");
        Assert.assertEquals(Long.class, sleepingSeconds.getDataType());
        Assert.assertEquals(TrackingMetricsConfig.Aggregation.SUM, sleepingSeconds.getAggregation());
        Assert.assertEquals(sleepingSeconds, trackingMetricsConfig.getTrackingMetric("SLEEPINGSECONDS"));

        TrackingMetricsConfig.TrackingMetric restingSeconds = trackingMetricMap.get("RESTINGSECONDS");
        Assert.assertEquals(Long.class, restingSeconds.getDataType());
        Assert.assertEquals(TrackingMetricsConfig.Aggregation.SUM, restingSeconds.getAggregation());
        Assert.assertEquals(restingSeconds, trackingMetricsConfig.getTrackingMetric("RESTINGSECONDS"));
    }

    //
    // getDataTypeFromProperties()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetDataTypeFromProperties_missing_data_type() {
        trackingMetricsConfig.getDataTypeFromProperties(5, trackingMetricsProperties, "something");
    }

    @Test
    public void testGetDataTypeFromProperties_expected() {

        Type dataType1 = trackingMetricsConfig.getDataTypeFromProperties(
                1, trackingMetricsProperties, "something");
        Assert.assertEquals(Long.class, dataType1);

        Type dataType4 = trackingMetricsConfig.getDataTypeFromProperties(
                4, trackingMetricsProperties, "something");
        Assert.assertEquals(Long.class, dataType4);
    }

    //
    // getAggregationFromProperties()
    //

    @Test(expected = IllegalArgumentException.class)
    public void testGetAggregationFromProperties_missing_aggregation() {
        trackingMetricsConfig.getAggregationFromProperties(5, trackingMetricsProperties, "something");
    }

    @Test
    public void testGetAggregationFromProperties_missing_expected() {

        TrackingMetricsConfig.Aggregation aggregation1 =
                trackingMetricsConfig.getAggregationFromProperties(1, trackingMetricsProperties, "something");
        Assert.assertEquals(TrackingMetricsConfig.Aggregation.SUM, aggregation1);

        TrackingMetricsConfig.Aggregation aggregation4 =
                trackingMetricsConfig.getAggregationFromProperties(4, trackingMetricsProperties, "something");
        Assert.assertEquals(TrackingMetricsConfig.Aggregation.SUM, aggregation4);
    }
}
