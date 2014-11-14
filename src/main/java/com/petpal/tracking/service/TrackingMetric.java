package com.petpal.tracking.service;

import java.util.ArrayList;
import java.util.List;

/**
 * This defines the names of all the metrics for which time series
 * data can be stored in the tracking data service.
 *
 * Created by per on 10/30/14.
 */
public enum TrackingMetric {

    WALKINGSTEPS,
    RUNNINGSTEPS,
    SLEEPINGSECONDS,
    RESTINGSECONDS;

    public static List<TrackingMetric> getAllTrackingMetrics() {
        List<TrackingMetric> allMetrics = new ArrayList<TrackingMetric>();
        allMetrics.add(TrackingMetric.WALKINGSTEPS);
        allMetrics.add(TrackingMetric.RUNNINGSTEPS);
        allMetrics.add(TrackingMetric.SLEEPINGSECONDS);
        allMetrics.add(TrackingMetric.RESTINGSECONDS);
        return allMetrics;
    }
}
