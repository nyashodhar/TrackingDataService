package com.petpal.tracking.integration;

import java.util.Arrays;
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
        return Arrays.asList(values());
    }
}
