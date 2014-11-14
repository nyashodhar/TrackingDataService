package com.petpal.tracking.integration;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by per on 11/3/14.
 */
public enum  TestTrackingMetric {

    WALKINGSTEPS,
    RUNNINGSTEPS,
    SLEEPINGSECONDS,
    RESTINGSECONDS;

    public static List<TestTrackingMetric> getAllTrackingMetrics() {
        List<TestTrackingMetric> allMetrics = new ArrayList<TestTrackingMetric>();
        allMetrics.add(TestTrackingMetric.WALKINGSTEPS);
        allMetrics.add(TestTrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TestTrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TestTrackingMetric.RESTINGSECONDS);
        return allMetrics;
    }
}
