package com.petpal.tracking.service;

import com.petpal.tracking.data.TrackingData;
import com.petpal.tracking.service.async.TrackingDataInsertionWorker;
import com.petpal.tracking.service.metrics.TimeSeriesMetric;
import com.petpal.tracking.service.tag.TimeSeriesTag;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by per on 10/28/14.
 */
@Component("trackingDataService")
public class TrackingDataServiceImpl implements AsyncTrackingDataInserter, TrackingDataService {

    private Logger logger = Logger.getLogger(this.getClass());

    @Autowired
    private BucketAggregationUtil bucketAggregationUtil;

    @Autowired
    private TimeSeriesFacade timeSeriesFacade;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * @see com.petpal.tracking.service.TrackingDataService#getAggregatedTimeSeries(java.util.Map, java.util.List, Long, Long, org.kairosdb.client.builder.TimeUnit, java.util.TimeZone, int, boolean)
     */
    @Override
    public Map<TrackingMetric, Map<Long, Long>> getAggregatedTimeSeries(
            Map<TimeSeriesTag, String> tags,
            List<TrackingMetric> trackingMetrics,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            TimeZone aggregationTimeZone,
            int resultBucketMultiplier,
            boolean verboseResponse) {

        if(CollectionUtils.isEmpty(trackingMetrics)) {
            throw new IllegalArgumentException("No tracking metrics provided");
        }

        Long shiftedUTCBegin = bucketAggregationUtil.
                getUTCShiftedBucketTimeStamp(utcBegin, aggregationTimeZone, resultBucketSize);
        Long shiftedUTCEnd = (utcEnd == null) ? null : bucketAggregationUtil.getUTCShiftedBucketTimeStamp(utcEnd, aggregationTimeZone, resultBucketSize);;

        //
        // The 'tracking metric' in the API level corresponds to a different time series level
        // tracking metric since the data is aggregated into different time series based on time
        // horizon. Therefore we need to map into the actual time series we are going to query,
        // and after we get the result, we will map it back the API level metric identifier
        // expected by the client.
        //
        Map<TimeSeriesMetric, TrackingMetric> metricMap = new HashMap<TimeSeriesMetric, TrackingMetric>();
        List<TimeSeriesMetric> timeSeriesMetrics = new ArrayList<TimeSeriesMetric>();

        for(TrackingMetric trackingMetric : trackingMetrics) {
            TimeSeriesMetric timeSeriesMetric = TimeSeriesMetric.getAggregatedTimeSeriesMetric(trackingMetric, resultBucketSize);
            timeSeriesMetrics.add(timeSeriesMetric);
            metricMap.put(timeSeriesMetric, trackingMetric);
        }

        Map<TimeSeriesMetric, Map<Long, Long>> results = timeSeriesFacade.queryMultipleTimeSeries(
                tags, timeSeriesMetrics, shiftedUTCBegin, shiftedUTCEnd, resultBucketSize, resultBucketMultiplier, verboseResponse);

        // Shift result back from the UTC relative result
        Map<TimeSeriesMetric, Map<Long, Long>> unshiftedResults = new HashMap<TimeSeriesMetric, Map<Long, Long>>();
        for(TimeSeriesMetric timeSeriesMetric : results.keySet()) {
            Map<Long, Long> unshiftedResult = bucketAggregationUtil.
                    shiftResultToAggregationTimeZone(results.get(timeSeriesMetric), aggregationTimeZone, resultBucketSize);
            unshiftedResults.put(timeSeriesMetric, unshiftedResult);
        }

        // Map the unshifted results back to the API level metric
        Map<TrackingMetric, Map<Long, Long>> mappedResults = new HashMap<TrackingMetric, Map<Long, Long>>();
        for(TimeSeriesMetric timeSeriesMetric : results.keySet()) {
            mappedResults.put(metricMap.get(timeSeriesMetric), unshiftedResults.get(timeSeriesMetric));
        }

        return mappedResults;
    }


    /**
     * @see com.petpal.tracking.service.TrackingDataService#storeTrackingData(java.util.Map, com.petpal.tracking.data.TrackingData)
     */
    @Override
    public void storeTrackingData(Map<TimeSeriesTag, String> tags, TrackingData trackingData) {

        logger.info("storeTrackingData(): trackingData=" + trackingData);

        //
        // TODO: TimeZone should be specified by the client, or if not there should be a better
        // way to determine the default timezone to aggregate relative to.
        //

        TimeZone aggregationTimeZone = TimeZone.getTimeZone("PST");

        threadPoolTaskExecutor.execute(new TrackingDataInsertionWorker(this, trackingData, tags, aggregationTimeZone));

        logger.info("Tracking data prepared for worker thread.");
    }

    /**
     * Called asynchronously when tracking data is inserted into the tracking service.
     * @see com.petpal.tracking.service.AsyncTrackingDataInserter#asyncTrackingDataInsert(com.petpal.tracking.data.TrackingData, java.util.Map, java.util.TimeZone)
     * @param trackingData
     * @param tags
     * @param timeZone
     */
    public void asyncTrackingDataInsert(
            TrackingData trackingData, Map<TimeSeriesTag, String> tags, TimeZone timeZone) {

        // Store the raw metrics
        storeRawMetrics(trackingData, tags);

        // Update all the aggregated series
        for(TrackingMetric trackingMetric : TrackingMetric.getAllTrackingMetrics()) {
            updateAggregatedSeriesForMetric(trackingMetric, trackingData.getDataForMetric(trackingMetric), tags, timeZone);
        }
    }

    private void storeRawMetrics(TrackingData trackingData, Map<TimeSeriesTag, String> tags) {

        MetricBuilder metricBuilder = MetricBuilder.getInstance();

        for(TrackingMetric trackingMetric : TrackingMetric.getAllTrackingMetrics()) {
            Map<Long, Long> dataPoints = trackingData.getDataForMetric(trackingMetric);
            if(!CollectionUtils.isEmpty(dataPoints)) {
                timeSeriesFacade.addTimeSeriesDataToMetricBuilder(metricBuilder, dataPoints, TimeSeriesMetric.getRawMetric(trackingMetric), tags);
            }
        }

        timeSeriesFacade.insertData(metricBuilder);
    }


    /**
     * Handles the storage of new unaggregated date for a given metric. All aggregated
     * series are updated before storing the raw aggregated as well.
     * @param trackingMetric the metric to which the new data is related
     * @param unaggregatedData the new data
     * @param tags the tags to use for the new data
     * @param timeZone the timezone to do the bucket aggregation relative to.
     */
    protected void updateAggregatedSeriesForMetric(TrackingMetric trackingMetric, Map<Long, Long> unaggregatedData, Map<TimeSeriesTag, String> tags, TimeZone timeZone) {

        if(CollectionUtils.isEmpty(unaggregatedData)) {
            logger.info("storeDataForMetric(): No unaggregated data found for metric " +
                    trackingMetric + ", returning.");
            return;
        }

        // Update all the aggregated data series for this metric
        updateUTCShiftedAggregatedTimeSeries(trackingMetric, unaggregatedData, tags, timeZone, TimeUnit.YEARS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetric, unaggregatedData, tags, timeZone, TimeUnit.MONTHS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetric, unaggregatedData, tags, timeZone, TimeUnit.WEEKS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetric, unaggregatedData, tags, timeZone, TimeUnit.DAYS);
        updateUTCShiftedAggregatedTimeSeries(trackingMetric, unaggregatedData, tags, timeZone, TimeUnit.HOURS);

    }


    /**
     * Takes care of updating the aggregated time series to incorporate the new data.
     * The data is shifted to fit into UTC timezone relative buckets and a 48hr transform
     * is applied to ensure the shifting into UTC never yields future data. The 48hr shift
     * is reversed by the layer that later queries for the aggregated data.
     * @param trackingMetric the metric to which the new data is related
     * @param unaggregatedData the new data
     * @param tags the tags to use for the new data
     * @param timeZone the timezone to do the bucket aggregation relative to.
     * @param bucketSize bucket size that is used to identify which time series to update
     *                   with the new data for this metric.s
     */
    protected void updateUTCShiftedAggregatedTimeSeries(TrackingMetric trackingMetric, Map<Long, Long> unaggregatedData,
                                              Map<TimeSeriesTag, String> tags, TimeZone timeZone, TimeUnit bucketSize) {

        // Aggregate the input into buckets for this aggregated series
        Map<Long, Long> fortyEightHourShiftedUTCAggregatedData = bucketAggregationUtil.
                aggregateIntoUTCShiftedBuckets(unaggregatedData, timeZone, bucketSize);

        if(CollectionUtils.isEmpty(fortyEightHourShiftedUTCAggregatedData)) {
            logger.info("updateUTCShiftedAggregatedTimeSeries(): No aggregated data found for metric " +
                    trackingMetric + ", returning.");
            return;
        }

        // Query for existing utc shifted aggregated data
        TimeSeriesMetric timeSeriesMetric = TimeSeriesMetric.getAggregatedTimeSeriesMetric(trackingMetric, bucketSize);
        List<TimeSeriesMetric> timeSeriesMetrics = new ArrayList<TimeSeriesMetric>();
        timeSeriesMetrics.add(timeSeriesMetric);

        long startOfFirstBucket = fortyEightHourShiftedUTCAggregatedData.keySet().iterator().next();

        Map<Long, Long> existingDataPoints =
                timeSeriesFacade.querySingleTimeSeries(tags, timeSeriesMetric, startOfFirstBucket, null, bucketSize, 1, false);

        // Incorporate the existing values for the contributed data points
        Map<Long, Long> updatedAggregatedData = bucketAggregationUtil.
                mergeExistingDataPointsIntoNew(fortyEightHourShiftedUTCAggregatedData, existingDataPoints);

        // Persist the updated data in the time series

        MetricBuilder metricBuilder = MetricBuilder.getInstance();
        timeSeriesFacade.addTimeSeriesDataToMetricBuilder(metricBuilder, updatedAggregatedData, timeSeriesMetric, tags);
        timeSeriesFacade.insertData(metricBuilder);
    }
}
