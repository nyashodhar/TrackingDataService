package com.petpal.tracking.service;

import com.petpal.tracking.web.controllers.AggregationLevel;
import com.petpal.tracking.web.controllers.TrackingMetric;
import org.springframework.util.Assert;

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
     * @param aggregationLevel the time series bucket size
     * @return name of a time series used to store aggregated data.
     */
    public static TimeSeriesMetric getAggregatedTimeSeriesMetric(TrackingMetric trackingMetric, AggregationLevel aggregationLevel) {
        Assert.notNull(trackingMetric, "Tracking metric missing");
        Assert.notNull(aggregationLevel, "Aggregation level missing");
        return TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + aggregationLevel.toString().toUpperCase());
    }

    /**
     * Get the names of a all the time series used to aggregate data for an API-level tracking metric identifier
     * @param trackingMetric the name of the tracking metric supplied to the rest API.
     * @return name of all time series used to store aggregated data for the given API-level tracking metric identifier.
     */
    public static List<TimeSeriesMetric> getAggregatedTimeSeriesMetrics(TrackingMetric trackingMetric) {

        Assert.notNull(trackingMetric, "Tracking metric missing");

        List<TimeSeriesMetric> aggregatedTrackingMetrics = new ArrayList<TimeSeriesMetric>();

        for(AggregationLevel aggregationLevel : AggregationLevel.getAllAggregationLevels()) {
            TimeSeriesMetric aggregatedTrackingMetric =
                TimeSeriesMetric.valueOf(trackingMetric.toString() + "_" + aggregationLevel.toString().toUpperCase());
            aggregatedTrackingMetrics.add(aggregatedTrackingMetric);
        }

        return aggregatedTrackingMetrics;
    }
}
