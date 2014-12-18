package com.petpal.tracking.service.timeseries;

import com.petpal.tracking.web.controllers.TrackingData;
import com.petpal.tracking.web.controllers.TrackingTag;
import org.kairosdb.client.builder.TimeUnit;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/2/14.
 */
public interface TimeSeriesFacade {

    /**
     * Perform a time series query for a single time series.
     * @param tags
     * @param timeSeriesName
     * @param dataType
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @param performBucketAdjustment
     * @return query results for a single time series.
     */
    TreeMap querySingleTimeSeries(
            Map<TrackingTag, String> tags,
            String timeSeriesName,
            Type dataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean performBucketAdjustment);

    /**
     * Perform a time series query for multiple time series (using the same range and tagging parameters for
     * each time series.
     * @param tags
     * @param timeSeriesNamesToDataType
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @param performBucketAdjustment
     * @return Query result, grouped by the metric for each time series.
     */
    Map<String, TreeMap> queryMultipleTimeSeries(
            Map<TrackingTag, String> tags,
            Map<String, Type> timeSeriesNamesToDataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean performBucketAdjustment);

    void storeDataForTimeSeries(TreeMap dataPoints, String timeSeriesName, Type dataType, Map<TrackingTag, String> tags);

    void storeRawMetrics(TrackingData trackingData, Map<TrackingTag, String> tags);


}
