package com.petpal.tracking.web.controllers;

import java.util.Arrays;
import java.util.List;

/**
 * This defines the names of all the metrics for which time series
 * data can be stored in the tracking data service.
 *
 * Created by per on 10/30/14.
 */
public enum TrackingMetric {

    WALKINGSTEPS(false),
    RUNNINGSTEPS(false),
    SLEEPINGSECONDS(false),
    RESTINGSECONDS(false);

    private final boolean isCalculatedAverage;

    TrackingMetric(boolean isCalculatedAverage) {
        this.isCalculatedAverage = isCalculatedAverage;
    }

    public boolean isCalculatedAverage() {
        return isCalculatedAverage;
    }

    public static List<TrackingMetric> getAllTrackingMetrics() {
        return Arrays.asList(values());
    }
}
