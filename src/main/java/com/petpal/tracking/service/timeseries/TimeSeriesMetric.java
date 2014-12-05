package com.petpal.tracking.service.timeseries;

import com.petpal.tracking.web.controllers.AggregationLevel;
import com.petpal.tracking.web.controllers.TrackingMetric;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by per on 11/12/14.
 */
public enum TimeSeriesMetric {

    WALKINGSTEPS_YEARS(false),
    WALKINGSTEPS_MONTHS(false),
    WALKINGSTEPS_WEEKS(false),
    WALKINGSTEPS_DAYS(false),
    WALKINGSTEPS_HOURS(false),
    RUNNINGSTEPS_YEARS(false),
    RUNNINGSTEPS_MONTHS(false),
    RUNNINGSTEPS_WEEKS(false),
    RUNNINGSTEPS_DAYS(false),
    RUNNINGSTEPS_HOURS(false),
    SLEEPINGSECONDS_YEARS(false),
    SLEEPINGSECONDS_MONTHS(false),
    SLEEPINGSECONDS_WEEKS(false),
    SLEEPINGSECONDS_DAYS(false),
    SLEEPINGSECONDS_HOURS(false),
    RESTINGSECONDS_YEARS(false),
    RESTINGSECONDS_MONTHS(false),
    RESTINGSECONDS_WEEKS(false),
    RESTINGSECONDS_DAYS(false),
    RESTINGSECONDS_HOURS(false),
    WALKINGSTEPS_RAW(false),
    RUNNINGSTEPS_RAW(false),
    SLEEPINGSECONDS_RAW(false),
    RESTINGSECONDS_RAW(false);

    private final boolean aggregateAsAverage;

    TimeSeriesMetric(boolean aggregateAsAverage) {
        this.aggregateAsAverage = aggregateAsAverage;
    }

    public boolean aggregateAsAverage() {
        return aggregateAsAverage;
    }

    /**
     * Get the name of a time series used for raw data given the API's tracking metric identifier
     * @param trackingMetric the name of the tracking metric supplied to the rest API.
     * @return name of a time series used to store raw data.
     */
    public static TimeSeriesMetric getRawMetric(TrackingMetric trackingMetric) {
        Assert.notNull(trackingMetric, "Tracking metric missing");
        return TimeSeriesMetric.valueOf(trackingMetric.toString() + "_RAW");
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
