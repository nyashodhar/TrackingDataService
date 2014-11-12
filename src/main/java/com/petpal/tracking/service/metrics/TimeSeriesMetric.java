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


    /**
     * Get the name of a time series used for raw data given the API's tracking metric identifier
     * @param trackingMetric the name of the tracking metric supplied to the rest API.
     * @return name of a time series used to store raw data.
     */
    public static TimeSeriesMetric getRawMetric(TrackingMetric trackingMetric) {
        if(trackingMetric == null) {
            throw new IllegalArgumentException("Tracking metric missing");
        }
        return TimeSeriesMetric.valueOf(trackingMetric.toString()+"_RAW");
    }


    /**
     * Get the names of a time series used to aggregate data for the given bucket size
     * given the API's tracking metric identifier
     * @param trackingMetric the name of the tracking metric supplied to the rest API.
     * @param timeUnit the time series bucket size
     * @return name of a time series used to store aggregated data.
     */
    public static TimeSeriesMetric getAggregatedTimeSeriesMetric(TrackingMetric trackingMetric, TimeUnit timeUnit) {

        if(trackingMetric == null) {
            throw new IllegalArgumentException("Tracking metric missing");
        }

        if(timeUnit == null) {
            throw new IllegalArgumentException("Time unit missing");
        }

        List<TimeUnit> validTimeUnits = getValidTimeUnits();

        if(timeUnit != null && !validTimeUnits.contains(timeUnit)) {
            throw new IllegalArgumentException("Invalid time unit " + timeUnit);
        }

        return TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + timeUnit.toString());
    }

    /**
     * Get the names of a all the time series used to aggregate data for an API-level tracking metric identifier
     * @param trackingMetric the name of the tracking metric supplied to the rest API.
     * @return name of all time series used to store aggregated data for the given API-level tracking metric identifier.
     */
    public static List<TimeSeriesMetric> getAggregatedTimeSeriesMetrics(TrackingMetric trackingMetric) {

        if(trackingMetric == null) {
            throw new IllegalArgumentException("Tracking metric missing");
        }

        List<TimeUnit> validTimeUnits = getValidTimeUnits();

        List<TimeSeriesMetric> aggregatedTrackingMetrics = new ArrayList<TimeSeriesMetric>();

        for(TimeUnit validTimeUnit : validTimeUnits) {
            TimeSeriesMetric aggregatedTrackingMetric =
                TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + validTimeUnit.toString());
            aggregatedTrackingMetrics.add(aggregatedTrackingMetric);
        }

        return aggregatedTrackingMetrics;
    }

    private static List<TimeUnit> getValidTimeUnits() {
        List<TimeUnit> validTimeUnits = new ArrayList<TimeUnit>();
        validTimeUnits.add(TimeUnit.YEARS);
        validTimeUnits.add(TimeUnit.MONTHS);
        validTimeUnits.add(TimeUnit.WEEKS);
        validTimeUnits.add(TimeUnit.DAYS);
        validTimeUnits.add(TimeUnit.HOURS);
        return validTimeUnits;
    }
}
