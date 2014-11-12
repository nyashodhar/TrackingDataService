package com.petpal.tracking.service;

import org.kairosdb.client.builder.TimeUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * This enumeration names metrics for all the actual time series that are
 * stored to back the types of metrics an end user can refer to in the API
 * via as the TrackingMetric enum.
 *
 * Created by per on 11/12/14.
 */
public enum AggregatedTrackingMetric {

    WALKINGSTEPS_YEARS,
    WALKINGSTEPS_MONTHS,
    WALKINGSTEPS_WEEKS,
    WALKINGSTEPS_DAYS,
    WALKINGSTEPS_HOURS,
    RUNNINGSTEPS_YEARS,
    RUNNINGSTEPS_MONTHS,
    RUNNINGSTEPS_WEEKS,
    RUNNINGSTEPS_DAYS,
    RUNNINGSTEPS_HOURS,
    SLEEPINGSECONDS_YEARS,
    SLEEPINGSECONDS_MONTHS,
    SLEEPINGSECONDS_WEEKS,
    SLEEPINGSECONDS_DAYS,
    SLEEPINGSECONDS_HOURS,
    RESTINGSECONDS_YEARS,
    RESTINGSECONDS_MONTHS,
    RESTINGSECONDS_WEEKS,
    RESTINGSECONDS_DAYS,
    RESTINGSECONDS_HOURS;


    public static List<AggregatedTrackingMetric> getAggregatedTrackingMetrics(TrackingMetric trackingMetric, TimeUnit timeUnit) {

        if(trackingMetric == null) {
            throw new IllegalArgumentException("Tracking metric missing");
        }

        List<TimeUnit> validTimeUnits = new ArrayList<TimeUnit>();
        validTimeUnits.add(TimeUnit.YEARS);
        validTimeUnits.add(TimeUnit.MONTHS);
        validTimeUnits.add(TimeUnit.WEEKS);
        validTimeUnits.add(TimeUnit.DAYS);
        validTimeUnits.add(TimeUnit.HOURS);

        if(timeUnit != null && !validTimeUnits.contains(timeUnit)) {
            throw new IllegalArgumentException("Invalid time unit " + timeUnit);
        }

        List<AggregatedTrackingMetric> aggregatedTrackingMetrics = new ArrayList<AggregatedTrackingMetric>();

        if(timeUnit != null) {
            AggregatedTrackingMetric aggregatedTrackingMetric =
                    AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + timeUnit.toString());
            aggregatedTrackingMetrics.add(aggregatedTrackingMetric);
        } else {
            for(TimeUnit validTimeUnit : validTimeUnits) {
                AggregatedTrackingMetric aggregatedTrackingMetric =
                        AggregatedTrackingMetric.valueOf(trackingMetric.toString() + "_" + validTimeUnit.toString());
                aggregatedTrackingMetrics.add(aggregatedTrackingMetric);
            }
        }

        return aggregatedTrackingMetrics;
    }
}
