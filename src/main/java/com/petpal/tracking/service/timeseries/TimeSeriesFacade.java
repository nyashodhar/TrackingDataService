package com.petpal.tracking.service.timeseries;

import com.petpal.tracking.web.controllers.TrackingData;
import com.petpal.tracking.web.controllers.TrackingTag;
import org.kairosdb.client.builder.TimeUnit;

import java.util.List;
import java.util.Map;

/**
 * Created by per on 12/2/14.
 */
public interface TimeSeriesFacade {

    /**
     * Perform a time series query for a single time series.
     * @param tags
     * @param timeSeriesMetric
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @param verboseResponse
     * @return query results for a single time series.
     */
    Map<Long, Long> querySingleTimeSeries(
            Map<TrackingTag, String> tags,
            TimeSeriesMetric timeSeriesMetric,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean verboseResponse);

    /**
     * Perform a time series query for multiple time series (using the same range and tagging parameters for
     * each time series.
     * @param tags
     * @param timeSeriesMetrics
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @param verboseResponse
     * @return Query result, grouped by the metric for each time series.
     */
    Map<TimeSeriesMetric, Map<Long, Long>> queryMultipleTimeSeries(
            Map<TrackingTag, String> tags,
            List<TimeSeriesMetric> timeSeriesMetrics,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean verboseResponse);

    void storeDataForTimeSeries(Map<Long, Long> timeSeriesData, TimeSeriesMetric timeSeriesMetric, Map<TrackingTag, String> tags);


    void storeRawMetrics(TrackingData trackingData, Map<TrackingTag, String> tags);


}
