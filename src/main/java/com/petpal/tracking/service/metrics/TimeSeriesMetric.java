package com.petpal.tracking.service.metrics;

import com.petpal.tracking.service.TrackingMetric;
import org.kairosdb.client.builder.TimeUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by per on 11/12/14.
 */
public enum TimeSeriesMetric {

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
    RESTINGSECONDS_HOURS,
    WALKINGSTEPS_RAW,
    RUNNINGSTEPS_RAW,
    SLEEPINGSECONDS_RAW,
    RESTINGSECONDS_RAW;


    public static TimeSeriesMetric getRawMetric(TrackingMetric trackingMetric) {
        if(trackingMetric == null) {
            throw new IllegalArgumentException("Tracking metric missing");
        }
        return TimeSeriesMetric.valueOf(trackingMetric.toString()+"_RAW");
    }

    public static List<TimeSeriesMetric> getAggregatedTimeSeriesMetrics(TrackingMetric trackingMetric, TimeUnit timeUnit) {

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

        List<TimeSeriesMetric> aggregatedTrackingMetrics = new ArrayList<TimeSeriesMetric>();

        if(timeUnit != null) {
            TimeSeriesMetric aggregatedTrackingMetric =
                    TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + timeUnit.toString());
            aggregatedTrackingMetrics.add(aggregatedTrackingMetric);
        } else {
            for(TimeUnit validTimeUnit : validTimeUnits) {
                TimeSeriesMetric aggregatedTrackingMetric =
                        TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + validTimeUnit.toString());
                aggregatedTrackingMetrics.add(aggregatedTrackingMetric);
            }
        }

        return aggregatedTrackingMetrics;
    }
}
