package com.petpal.tracking.service;

import com.petpal.tracking.data.TrackingData;
import com.petpal.tracking.service.tag.TimeSeriesTag;
import org.kairosdb.client.builder.TimeUnit;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by per on 11/18/14.
 */
public interface TrackingDataService {

    /**
     * Query data for multiple API level metrics for a given time range and time horizon
     * based on aggregated time series data.
     * @param tags
     * @param trackingMetrics
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param aggregationTimeZone
     * @param resultBucketMultiplier
     * @param verboseResponse
     * @return time series data obtained from queries against aggregated time series.
     */
    Map<TrackingMetric, Map<Long, Long>> getAggregatedTimeSeries(
            Map<TimeSeriesTag, String> tags,
            List<TrackingMetric> trackingMetrics,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            TimeZone aggregationTimeZone,
            int resultBucketMultiplier,
            boolean verboseResponse);

    /**
     * Store tracking data. The raw data itself is persisted, but at
     * the same time, multiple aggregated time series are updated for each
     * metric. This makes is possible to later query the time series data
     * from those aggregated series, yielding a much fast query.
     *
     * @param tags the tags to apply to all the datapoints in the tracking data.
     * @param trackingData the data to be inserted into the time series
     *                     date store.
     * @param aggregationTimeZone timezone to aggregate the tracking data for
     */
    void storeTrackingData(Map<TimeSeriesTag, String> tags, TrackingData trackingData, TimeZone aggregationTimeZone);
}
