package com.petpal.tracking.service;

import com.petpal.tracking.data.TrackingData;
import com.petpal.tracking.service.util.QueryArgumentValidationUtil;
import com.petpal.tracking.service.util.QueryLoggingUtil;
import org.apache.commons.lang.math.LongRange;
import org.apache.log4j.Logger;
import org.kairosdb.client.KairosClientUtil;
import org.kairosdb.client.KairosRestClient;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import org.kairosdb.client.response.Results;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Created by per on 10/28/14.
 */
@Component
public class TrackingDataService {

    // TAG NAMES
    public static final String TAG_TRACKED_ENTITY = "trackedentity";
    public static final String TAG_TRACKING_DEVICE = "trackingdevice";

    private Logger logger = Logger.getLogger(this.getClass());

    @Value("${trackingService.kairosDBHost}")
    private String kairosDBHost;

    @Value("${trackingService.kairosDBPort}")
    private String kairosDBPort;

    private KairosRestClient kairosRestClient;

    public void storeTrackingData(TrackingData trackingData) {

        System.out.println("TrackingService.storeTrackingData(): Hello - kairosDBHost=" + kairosDBHost + ", kairosDBPort=" + kairosDBPort);
        logger.info("storeTrackingData(): trackingData=" + trackingData);
        logger.info("storeTrackingData(): HELLO THERE");

        Map<String, String> tags = new HashMap<String, String>();
        tags.put(TAG_TRACKED_ENTITY, trackingData.getTrackedEntityId());
        tags.put(TAG_TRACKING_DEVICE, trackingData.getTrackingDeviceId());

        MetricBuilder metricBuilder = MetricBuilder.getInstance();

        addMetricsToBuilder(metricBuilder, trackingData.getWalkingData(), TrackingMetric.WALKINGSTEPS, tags);
        addMetricsToBuilder(metricBuilder, trackingData.getRunningData(), TrackingMetric.RUNNINGSTEPS, tags);
        addMetricsToBuilder(metricBuilder, trackingData.getSleepingData(), TrackingMetric.SLEEPINGSECONDS, tags);
        addMetricsToBuilder(metricBuilder, trackingData.getRestingData(), TrackingMetric.RESTINGSECONDS, tags);

        Response response = KairosClientUtil.pushMetrics(metricBuilder, kairosRestClient);
        logger.info("Metrics pushed, response.getStatusCode() = " + response.getStatusCode());
    }


    public Map<TrackingMetric, Map<Long, Long>> getMetricsForAbsoluteRange(
            Map<String, String> tags,
            List<TrackingMetric> trackingMetrics,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean verboseResponse) {

        // Do some validation of the input parameters
        QueryArgumentValidationUtil.validateMetricsQueryParameters(
                tags, trackingMetrics, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Print a description of the query in the log
        QueryLoggingUtil.logTimeSeriesQueryDescription(tags, trackingMetrics, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Set the time interval for the query
        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        queryBuilder.setStart(new Date(utcBegin));
        if(utcEnd != null) {
            queryBuilder.setEnd(new Date(utcEnd));
        }

        // Add metrics, aggregator and tags to the query
        addMetricsAndAggregator(queryBuilder, trackingMetrics, resultBucketSize, resultBucketMultiplier, tags);

        // Do the query
        QueryResponse queryResponse = KairosClientUtil.executeQuery(queryBuilder, kairosRestClient);

        logger.info("getMetrics(): Query completed with status code " + queryResponse.getStatusCode());

        // Extract the result for response
        Map<TrackingMetric, Map<Long, Long>> metricResults = getMetricsResultFromResponse(queryResponse);

        QueryLoggingUtil.printMetricsResults(metricResults);
        logger.info("Doing result adjustment..");

        // Adjust the bucket boundaries (and insert explicit empty buckets is response is to be verbose)
        adjustBucketBoundaries(metricResults, utcBegin, utcEnd, resultBucketSize, verboseResponse);

        QueryLoggingUtil.printMetricsResults(metricResults);

        return metricResults;
    }


    public Map<TrackingMetric, Map<Long, Long>> getMetrics(Map<String, String> tags,
                           List<TrackingMetric> trackingMetrics,
                           int beginUnitsIntoPast,
                           Integer endUnitsIntoPast,
                           TimeUnit queryIntervalTimeUnit,
                           TimeUnit resultTimeUnit,
                           int resultBucketMultiplier) {

        // Do some validation of the input parameters
        QueryArgumentValidationUtil.validateMetricsQueryParameters(tags, trackingMetrics, beginUnitsIntoPast, endUnitsIntoPast,
                queryIntervalTimeUnit, resultTimeUnit, resultBucketMultiplier);

        // Print a description of the query in the log
        QueryLoggingUtil.logTimeSeriesQueryDescription(tags, trackingMetrics, beginUnitsIntoPast, endUnitsIntoPast,
                queryIntervalTimeUnit, resultTimeUnit, resultBucketMultiplier);

        QueryBuilder queryBuilder = QueryBuilder.getInstance();

        // Set the time interval for the query
        queryBuilder.setStart(beginUnitsIntoPast, queryIntervalTimeUnit);
        if(endUnitsIntoPast != null) {
            queryBuilder.setEnd(endUnitsIntoPast, queryIntervalTimeUnit);
        }

        // Add metrics, aggregator and tags to the query
        addMetricsAndAggregator(queryBuilder, trackingMetrics, resultTimeUnit, resultBucketMultiplier, tags);

        // Do the query
        QueryResponse queryResponse = KairosClientUtil.executeQuery(queryBuilder, kairosRestClient);

        logger.info("getMetrics(): Query completed with status code " + queryResponse.getStatusCode());

        // Extract the result for response
        Map<TrackingMetric, Map<Long, Long>> metricResults = getMetricsResultFromResponse(queryResponse);

        QueryLoggingUtil.printMetricsResults(metricResults);

        return metricResults;
    }

    private void addMetricsAndAggregator(QueryBuilder queryBuilder, List<TrackingMetric> trackingMetrics,
                                         TimeUnit resultBucketSize, int resultBucketMultiplier, Map<String, String> tags) {
        for(TrackingMetric trackingMetric : trackingMetrics) {
            QueryMetric queryMetric = queryBuilder.addMetric(trackingMetric.toString());
            queryMetric.addAggregator(AggregatorFactory.createSumAggregator(resultBucketMultiplier, resultBucketSize));
            for(String tag : tags.keySet()) {
                queryMetric.addTag(tag, tags.get(tag));
            }
        }
    }

    private Map<TrackingMetric, Map<Long, Long>> getMetricsResultFromResponse(QueryResponse queryResponse) {

        //
        // Gather the results into a data structure
        //
        // There will be one query executed for each metric that was included in the query builder
        //

        List<Queries> queries = queryResponse.getQueries();

        Map<TrackingMetric, Map<Long, Long>> metricResults = new HashMap<TrackingMetric, Map<Long, Long>>();

        for(Queries query : queries) {
            List<Results> results = query.getResults();
            for(Results result : results) {
                List<DataPoint> dataPoints = result.getDataPoints();
                String resultName = result.getName();
                TrackingMetric trackingMetric = TrackingMetric.valueOf(resultName.toUpperCase());
                if(!dataPoints.isEmpty()) {
                    if (!metricResults.containsKey(resultName)) {
                        metricResults.put(trackingMetric, new TreeMap<Long, Long>());
                    }
                }

                for(DataPoint dataPoint : dataPoints) {
                    metricResults.get(trackingMetric).put(dataPoint.getTimestamp(), KairosClientUtil.getLongValueFromDataPoint(dataPoint));
                }
            }
        }

        return metricResults;
    }


    private void adjustBucketBoundaries(Map<TrackingMetric, Map<Long, Long>> metricResults,
                                        Long utcBegin, Long utcEnd, TimeUnit resultBucketSize, boolean verboseResponse) {

        for (TrackingMetric trackingMetric : metricResults.keySet()) {

            //
            // Adjust the bucket boundaries and inject empty buckets.
            //
            Map<Long, Long> adjustedMetricResult = adjustBoundariesForMetricResult(
                    trackingMetric, metricResults.get(trackingMetric), utcBegin, utcEnd, resultBucketSize);
            metricResults.put(trackingMetric, adjustedMetricResult);

            // If the response is not to be verbose, filter out the empty buckets.
            if(!verboseResponse) {
                Set<Long> bucketTimeStamps = new HashSet<Long>(adjustedMetricResult.keySet());
                for(Long timestamp : bucketTimeStamps) {
                    if(adjustedMetricResult.get(timestamp) == 0L) {
                        adjustedMetricResult.remove(timestamp);
                    }
                }
            }
        }
    }


    //
    // When aggregating the output into buckets, the timestamp of the bucket is unfortunately
    // NOT equal to the start of the bucket interval (which would have been logical). Rather
    // the timestamp of the sum in a bucket is equal to the first timestamp for which a match
    // was found in the bucket interval.
    //
    // This method adjusts the bucket starts of each bucket.
    //
    // For bucket intervals for which no data was found, the result does not include a bucket.
    // This method will insert an empty bucket tied to the start of the interval for which
    // no data was found.
    //
    // Example:
    //    Database has datapoints May 29 - 3 steps, July 2nd 2 steps
    // Query:
    //    Get data starting May 1st - Aug 1
    // Result
    //    Bucket 1 - timestamp May 29 - value/sum = 3
    //    Bucket 2 - timestamp July 2nd - value/sum = 3
    // After transform:
    //    Bucket 1 - timestamp May 1 - value/sum = 3
    //    Bucket 2 - timestamp June 1 - value/sum = 0
    //    Bucket 2 - timestamp July 1 - value/sum = 3
    //
    private Map<Long, Long> adjustBoundariesForMetricResult(
            TrackingMetric trackingMetric, Map<Long, Long> metricResult, Long utcBegin, Long utcEnd, TimeUnit resultBucketSize) {

        if(utcBegin == null) {
            throw new IllegalArgumentException("utcBegin not specified");
        }

        if(utcEnd != null && (utcEnd.longValue() <= utcBegin.longValue())) {
            throw new IllegalArgumentException("The utcEnd value ("+utcEnd+") must be greater than utcBegin value (" + utcBegin + ")");
        }

        Calendar cursorCalendar = new GregorianCalendar();
        cursorCalendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        cursorCalendar.setTimeInMillis(utcBegin);

        long endOfInterval = (utcEnd == null) ? System.currentTimeMillis() : utcEnd;

        //
        // Generate all the correct bucket boundaries
        // This will also ensure that any missing empty buckets are created.
        //

        Map<Long, Long> newMetricResult = new TreeMap<Long, Long>();

        while(true) {
            newMetricResult.put(cursorCalendar.getTimeInMillis(), 0L);

            //
            // Step one bucket size unit forward. If we're beyond
            // the end of the interval don't add any more buckets.
            //
            int calenderUnitToStepForward = getCalendarUnitForTimeUnit(resultBucketSize);
            cursorCalendar.add(calenderUnitToStepForward, 1);
            if(cursorCalendar.getTimeInMillis() >= endOfInterval) {
                break;
            }
        }

        logTimeStampAdjustments(trackingMetric, metricResult, newMetricResult);

        //
        // Walk through all the previous results, and drop them into the correct corresponding bucket
        // TODO: Since it's a treemap, we can do this in linear time but for now just brute attack...
        //

        List<Long> newBucketTimeStamps = new ArrayList<Long>(newMetricResult.size());
        newBucketTimeStamps.addAll(newMetricResult.keySet());

        List<LongRange> newBucketRanges = new ArrayList<LongRange>(newMetricResult.size());

        for (int i = 0; i < newBucketTimeStamps.size(); i++) {
            long timeStampAtIndex = newBucketTimeStamps.get(i);
            if(i == (newBucketTimeStamps.size()-1)) {
                newBucketRanges.add(new LongRange(timeStampAtIndex, endOfInterval));
            } else {
                newBucketRanges.add(new LongRange(timeStampAtIndex, newBucketTimeStamps.get(i+1)-1));
            }
        }

        for(long oldTimeStamp : metricResult.keySet()) {
            for (LongRange newBucketRange : newBucketRanges) {
                if (newBucketRange.containsLong(oldTimeStamp)) {
                    newMetricResult.put(newBucketRange.getMinimumLong(), metricResult.get(oldTimeStamp));
                }
            }
        }

        return newMetricResult;
    }

    private int getCalendarUnitForTimeUnit(TimeUnit timeUnit) {

        if(timeUnit == TimeUnit.YEARS) {
            return Calendar.YEAR;
        } else if(timeUnit == TimeUnit.MONTHS) {
            return Calendar.MONTH;
        } else if(timeUnit == TimeUnit.WEEKS) {
            return Calendar.WEEK_OF_YEAR;
        } else if(timeUnit == TimeUnit.DAYS) {
            return Calendar.DATE;
        } else if(timeUnit == TimeUnit.HOURS) {
            return Calendar.HOUR;
        } else if(timeUnit == TimeUnit.MINUTES) {
            return Calendar.MINUTE;
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit " + timeUnit);
        }
    }

    private void logTimeStampAdjustments(TrackingMetric trackingMetric, Map<Long, Long> metricResult, Map<Long, Long> newMetricResult) {

        logger.debug("Bucket adjust for metric " + trackingMetric + ": old bucket count = " + metricResult.size() + ", new bucket count = " + newMetricResult.size());

        StringBuilder oldTimeStamps = new StringBuilder();
        for(long oldTimeStamp : metricResult.keySet()) {
            if(oldTimeStamps.length() > 0) {
                oldTimeStamps.append(", ");
            }
            oldTimeStamps.append(QueryLoggingUtil.getUTCFormat(oldTimeStamp) + "(" + oldTimeStamp  + ")");
        }

        logger.debug("Old bucket boundaries: [" + oldTimeStamps + "]");

        StringBuilder newTimeStamps = new StringBuilder();
        for(long newTimeStamp : newMetricResult.keySet()) {
            if(newTimeStamps.length() > 0) {
                newTimeStamps.append(", ");
            }
            newTimeStamps.append(QueryLoggingUtil.getUTCFormat(newTimeStamp) + "(" + newTimeStamp  + ")");
        }

        logger.debug("New bucket boundaries: [" + newTimeStamps + "]");
    }


    private void addMetricsToBuilder(MetricBuilder metricBuilder, Map<Long, Long> timeStampValueMap, TrackingMetric trackingMetric, Map<String, String> tags) {

        Metric metric = metricBuilder.addMetric(trackingMetric.toString());
        for(String tagName : tags.keySet()) {
            metric.addTag(tagName, tags.get(tagName));
        }

        for(long timeStamp : timeStampValueMap.keySet()) {
            long metricValue = timeStampValueMap.get(timeStamp);
            metric.addDataPoint(timeStamp, metricValue);
        }

        logger.info("Metric " + trackingMetric + " prepped: timeStampValueMap = " + timeStampValueMap + ", tags = " + tags);
    }

    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        String kairosDBURL = "http://" + kairosDBHost + ":" + kairosDBPort;
        kairosRestClient = new KairosRestClient(kairosDBURL);
    }

}
