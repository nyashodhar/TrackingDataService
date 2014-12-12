package com.petpal.tracking.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by per on 12/11/14.
 */
public class TrackingMetricConfigUtil {

    public static final String METRIC_WALKING_STEPS = "WALKINGSTEPS";
    public static final String METRIC_RUNNING_STEPS = "RUNNINGSTEPS";
    public static final String METRIC_SLEEPING_SECONDS = "SLEEPINGSECONDS";
    public static final String METRIC_RESTING_SECONDS = "RESTINGSECONDS";

    private static final List<String> LONG_METRICS = new ArrayList<String>();

    static {
        LONG_METRICS.add(METRIC_WALKING_STEPS);
        LONG_METRICS.add(METRIC_RUNNING_STEPS);
        LONG_METRICS.add(METRIC_SLEEPING_SECONDS);
        LONG_METRICS.add(METRIC_RESTING_SECONDS);
    }

    public static List<String> getAllLongTypeMetrics() {
        return LONG_METRICS;
    }
}


