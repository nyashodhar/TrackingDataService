package com.petpal.tracking.integration;

/**
 * Created by per on 11/3/14.
 */
public enum TestTrackingMetric {

    WALKINGSTEPS,
    RUNNINGSTEPS,
    SLEEPINGSECONDS,
    RESTINGSECONDS;

    public String toString() {
        return name().toLowerCase();
    }
}
