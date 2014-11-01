package com.petpal.tracking.service;

/**
 * Created by per on 10/30/14.
 */
public enum TrackingMetric {

    WALKINGSTEPS,
    RUNNINGSTEPS,
    SLEEPINGSECONDS,
    RESTINGSECONDS;

    public String toString() {
        return name().toLowerCase();
    }
}
