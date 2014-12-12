package com.petpal.tracking.service;

import com.petpal.tracking.service.async.AsyncTrackingDataInserter;
import com.petpal.tracking.service.async.TrackingDataInsertionWorker;
import com.petpal.tracking.service.timeseries.TimeSeriesFacade;
import com.petpal.tracking.web.controllers.AggregationLevel;
import com.petpal.tracking.web.controllers.TrackingData;
import com.petpal.tracking.web.controllers.TrackingDataDownload;
import com.petpal.tracking.web.controllers.TrackingTag;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Created by per on 10/28/14.
 */
@Component("trackingDataService")
public class TrackingDataServiceImpl implements AsyncTrackingDataInserter, TrackingDataService {

    private Logger logger = Logger.getLogger(this.getClass());

    @Autowired
    private BucketAggregationUtil bucketAggregationUtil;

    @Autowired
    @Qualifier("timeSeriesFacade")
    private TimeSeriesFacade timeSeriesFacade;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    @Autowired
    private TrackingMetricsConfig trackingMetricsConfig;

    /**
     * @see com.petpal.tracking.service.TrackingDataService#getAggregatedTimeSeries(java.util.Map, java.util.List, Long, Long, com.petpal.tracking.web.controllers.AggregationLevel, java.util.TimeZone, int, boolean)
     */
    @Override
    public TrackingDataDownload getAggregatedTimeSeries(
            Map<TrackingTag, String> tags,
            List<TrackingMetricConfig> trackingMetricConfigs,
            Long utcBegin,
            Long utcEnd,
            AggregationLevel aggregationLevel,
            TimeZone aggregationTimeZone,
            int resultBucketMultiplier,
            boolean verboseResponse) {

        Assert.notNull(aggregationLevel, "Aggregation level not specified");

        if(CollectionUtils.isEmpty(trackingMetricConfigs)) {
            throw new IllegalArgumentException("No tracking metrics provided");
        }

        Long shiftedUTCBegin = bucketAggregationUtil.
                getUTCShiftedBucketTimeStamp(utcBegin, aggregationTimeZone, aggregationLevel);
        Long shiftedUTCEnd = (utcEnd == null) ? null : bucketAggregationUtil.getUTCShiftedBucketTimeStamp(utcEnd, aggregationTimeZone, aggregationLevel);;

        //
        // The 'tracking metric' in the API level corresponds to a different time series level
        // tracking metric since the data is aggregated into different time series based on time
        // horizon. Therefore we need to map into the actual time series we are going to query,
        // and after we get the result, we will map it back the API level metric identifier
        // expected by the client.
        //
        Map<String, TrackingMetricConfig> metricMap = new HashMap<String, TrackingMetricConfig>();
        Map<String, Type> timeSeriesNamesToDataType = new HashMap<String, Type>();

        for(TrackingMetricConfig trackingMetricConfig : trackingMetricConfigs) {
            String timeSeriesName = trackingMetricConfig.getAggregatedSeriesName(aggregationLevel);
            Type timeSeriesType = trackingMetricConfig.getDataType();
            timeSeriesNamesToDataType.put(timeSeriesName, timeSeriesType);
            metricMap.put(timeSeriesName, trackingMetricConfig);
        }

        TimeUnit timeUnitForAggregationLevel = TimeUnit.valueOf(aggregationLevel.toString().toUpperCase());

        Map<String, TreeMap> results = timeSeriesFacade.queryMultipleTimeSeries(
                tags,
                timeSeriesNamesToDataType,
                shiftedUTCBegin,
                shiftedUTCEnd,
                timeUnitForAggregationLevel,
                resultBucketMultiplier,
                verboseResponse);

        // Shift result back from the UTC relative result
        Map<String, TreeMap> unshiftedResults = new HashMap<String, TreeMap>();
        for(String timeSeriesName : results.keySet()) {
            TreeMap unshiftedResult = bucketAggregationUtil.
                    shiftResultToAggregationTimeZone(results.get(timeSeriesName), aggregationTimeZone, aggregationLevel);
            unshiftedResults.put(timeSeriesName, unshiftedResult);
        }

        // Map the unshifted results back to the API level metric
        Map<String, TreeMap> mappedResults = new HashMap<String, TreeMap>();
        for(String timeSeriesName : results.keySet()) {
            mappedResults.put(metricMap.get(timeSeriesName).getName(), unshiftedResults.get(timeSeriesName));
        }

        // Create a response object
        return createTrackingDataDownload(mappedResults);
    }


    /**
     * @see com.petpal.tracking.service.TrackingDataService#storeTrackingData(java.util.Map, com.petpal.tracking.web.controllers.TrackingData, java.util.TimeZone)
     */
    @Override
    public void storeTrackingData(Map<TrackingTag, String> tags, TrackingData trackingData, TimeZone aggregationTimeZone) {
        logger.info("storeTrackingData(): trackingData=" + trackingData);
        threadPoolTaskExecutor.execute(new TrackingDataInsertionWorker(this, trackingData, tags, aggregationTimeZone));
        logger.info("Tracking data prepared for worker thread.");
    }

    /**
     * Called asynchronously when tracking data is inserted into the tracking service.
     * @see com.petpal.tracking.service.async.AsyncTrackingDataInserter#asyncTrackingDataInsert(com.petpal.tracking.web.controllers.TrackingData, java.util.Map, java.util.TimeZone)      * @param trackingData
     * @param tags
     * @param timeZone
     */
    public void asyncTrackingDataInsert(
            TrackingData trackingData, Map<TrackingTag, String> tags, TimeZone timeZone) {

        // Store the raw metrics
        timeSeriesFacade.storeRawMetrics(trackingData, tags);

        // Update all the aggregated series
        Map<TrackingMetricConfig, TreeMap> metricConfigAndData = trackingData.getMetricConfigAndData();

        for(TrackingMetricConfig trackingMetricConfig : metricConfigAndData.keySet()) {
            updateAggregatedSeriesForMetric(
                    trackingMetricConfig, metricConfigAndData.get(trackingMetricConfig), tags, timeZone);
        }
    }

    /**
     * Handles the storage of new unaggregated date for a given metric. All aggregated
     * series are updated before storing the raw aggregated as well.
     * @param trackingMetricConfig the metric to which the new data is related
     * @param unaggregatedDataPoints the new data
     * @param tags the tags to use for the new data
     * @param timeZone the timezone to do the bucket aggregation relative to.
     */
    protected void updateAggregatedSeriesForMetric(
            TrackingMetricConfig trackingMetricConfig,
            TreeMap unaggregatedDataPoints,
            Map<TrackingTag, String> tags,
            TimeZone timeZone) {

        if(CollectionUtils.isEmpty(unaggregatedDataPoints)) {
            logger.info("updateAggregatedSeriesForMetric(): No unaggregated data found for " +
                    trackingMetricConfig.getDataType() + " metric " + trackingMetricConfig.getName() + ", returning.");
            return;
        }

        // Update all the aggregated data series for this metric
        updateUTCShiftedAggregatedTimeSeries(trackingMetricConfig, unaggregatedDataPoints, tags, timeZone, AggregationLevel.YEARS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetricConfig, unaggregatedDataPoints, tags, timeZone, AggregationLevel.MONTHS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetricConfig, unaggregatedDataPoints, tags, timeZone, AggregationLevel.WEEKS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetricConfig, unaggregatedDataPoints, tags, timeZone, AggregationLevel.DAYS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetricConfig, unaggregatedDataPoints, tags, timeZone, AggregationLevel.HOURS);
    }


    /**
     * Takes care of updating the aggregated time series to incorporate the new data.
     * The data is shifted to fit into UTC timezone relative buckets and a 48hr transform
     * is applied to ensure the shifting into UTC never yields future data. The 48hr shift
     * is reversed by the layer that later queries for the aggregated data.
     * @param trackingMetricConfig the metric to which the new data is related
     * @param unaggregatedDataPoints the new data
     * @param tags the tags to use for the new data
     * @param timeZone the timezone to do the bucket aggregation relative to.
     * @param aggregationLevel bucket size that is used to identify which time series to update
     *                   with the new data for this metric.
     */
    protected void updateUTCShiftedAggregatedTimeSeries(TrackingMetricConfig trackingMetricConfig, TreeMap unaggregatedDataPoints,
                                              Map<TrackingTag, String> tags, TimeZone timeZone, AggregationLevel aggregationLevel) {

        // Aggregate the input into buckets for this aggregated series
        TreeMap fortyEightHourShiftedUTCAggregatedData = bucketAggregationUtil.
                aggregateIntoUTCShiftedBuckets(trackingMetricConfig, unaggregatedDataPoints, timeZone, aggregationLevel);

        if(CollectionUtils.isEmpty(fortyEightHourShiftedUTCAggregatedData)) {
            logger.info("updateUTCShiftedAggregatedTimeSeries(): No aggregated data found for " +
                    trackingMetricConfig.getDataType() + " metric " + trackingMetricConfig.getName() + ", returning.");
            return;
        }

        // Query for existing utc shifted aggregated data

        String timeSeriesName = trackingMetricConfig.getAggregatedSeriesName(aggregationLevel);

        Long startOfFirstBucket = (Long) fortyEightHourShiftedUTCAggregatedData.keySet().iterator().next();

        TimeUnit timeUnitForAggregationLevel = TimeUnit.valueOf(aggregationLevel.toString().toUpperCase());

        TreeMap existingDataPoints = timeSeriesFacade.querySingleTimeSeries(
                tags, timeSeriesName, trackingMetricConfig.getDataType(), startOfFirstBucket, null, timeUnitForAggregationLevel, 1, false);

        // Incorporate the existing values for the contributed data points
        TreeMap updatedAggregatedData = bucketAggregationUtil.mergeExistingDataPointsIntoNew(
                fortyEightHourShiftedUTCAggregatedData,
                existingDataPoints,
                trackingMetricConfig.getAggregation(),
                trackingMetricConfig.getDataType());

        // Persist the updated data in the time series
        timeSeriesFacade.storeDataForTimeSeries(updatedAggregatedData, timeSeriesName, trackingMetricConfig.getDataType(), tags);
    }


    private TrackingDataDownload createTrackingDataDownload(Map<String, TreeMap> mappedResults) {

        Map<String, TreeMap> longMetrics = new HashMap<String, TreeMap>();
        Map<String, TreeMap> doubleMetrics = new HashMap<String, TreeMap>();
        Map<String, TreeMap> stringMetrics = new HashMap<String, TreeMap>();

        for (String metricName : mappedResults.keySet()) {
            TreeMap dataPoints = mappedResults.get(metricName);
            Object obj = dataPoints.values().iterator().next();
            if (obj instanceof Long) {
                longMetrics.put(metricName, dataPoints);
            } else if (obj instanceof Double) {
                doubleMetrics.put(metricName, dataPoints);
            } else if (obj instanceof String) {
                stringMetrics.put(metricName, dataPoints);
            }
        }

        TrackingDataDownload trackingDataDownload = new TrackingDataDownload();
        trackingDataDownload.setLongMetrics(longMetrics);
        return trackingDataDownload;
    }

}
