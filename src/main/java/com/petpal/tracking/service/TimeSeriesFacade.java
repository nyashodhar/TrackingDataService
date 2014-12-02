package com.petpal.tracking.service;

import com.petpal.tracking.service.util.BucketBoundaryUtil;
import com.petpal.tracking.web.controllers.TrackingTag;
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
import org.springframework.beans.factory.annotation.Autowired;
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
 * Contains operations to create queries and extract results from queries
 * Created by per on 11/12/14.
 */
@Component
public class TimeSeriesFacade {

    private Logger logger = Logger.getLogger(this.getClass());

    @Value("${trackingService.kairosDBHost}")
    private String kairosDBHost;

    @Value("${trackingService.kairosDBPort}")
    private String kairosDBPort;

    private KairosRestClient kairosRestClient;

    @Autowired
    private BucketAggregationUtil bucketAggregationUtil;

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
    public Map<Long, Long> querySingleTimeSeries(
            Map<TrackingTag, String> tags,
            TimeSeriesMetric timeSeriesMetric,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean verboseResponse) {

        // Build the query
        QueryBuilder queryBuilder = createQueryForSingleMetric(
                tags, timeSeriesMetric, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Do the query
        QueryResponse queryResponse = KairosClientUtil.executeQuery(queryBuilder, kairosRestClient);
        logger.debug("querySingleTimeSeries(): Query completed with status code " +
                queryResponse.getStatusCode() + " for series " + timeSeriesMetric);

        // Extract the result for response
        Map<TimeSeriesMetric, Map<Long, Long>> resultsByMetric = getResultFromResponse(queryResponse);
        QueryLoggingUtil.printMetricsResults(resultsByMetric);

        // Adjust the bucket boundaries (and insert explicit empty buckets is response is to be verbose)
        logger.debug("querySingleTimeSeries(): Doing result adjustment for series " + timeSeriesMetric);
        adjustBucketBoundaries(resultsByMetric, utcBegin, utcEnd, resultBucketSize, verboseResponse);
        QueryLoggingUtil.printMetricsResults(resultsByMetric);

        Map<Long, Long> results = resultsByMetric.get(timeSeriesMetric);
        logger.debug("querySingleTimeSeries(): results = " + results);

        return results;
    }

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
    public Map<TimeSeriesMetric, Map<Long, Long>> queryMultipleTimeSeries(
            Map<TrackingTag, String> tags,
            List<TimeSeriesMetric> timeSeriesMetrics,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean verboseResponse) {

        // Build the query
        QueryBuilder queryBuilder = createQueryForMultipleMetrics(
                tags, timeSeriesMetrics, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Do the query
        QueryResponse queryResponse = KairosClientUtil.executeQuery(queryBuilder, kairosRestClient);
        logger.info("queryMultipleTimeSeries(): Query completed with status code " + queryResponse.getStatusCode());

        // Extract the result for response
        Map<TimeSeriesMetric, Map<Long, Long>> resultsByMetric = getResultFromResponse(queryResponse);
        QueryLoggingUtil.printMetricsResults(resultsByMetric);

        // Adjust the bucket boundaries (and insert explicit empty buckets is response is to be verbose)
        logger.info("queryMultipleTimeSeries(): Doing result adjustment..");
        adjustBucketBoundaries(resultsByMetric, utcBegin, utcEnd, resultBucketSize, verboseResponse);
        QueryLoggingUtil.printMetricsResults(resultsByMetric);

        return resultsByMetric;
    }


    /**
     * Persists data for a time series
     * @param metricBuilder
     * @return a metric builder with data than can be passed to the kairos db
     * client to be persisted.
     */
    public void insertData(MetricBuilder metricBuilder) {

        if(metricBuilder == null) {
            throw new IllegalArgumentException("No metric builder provided");
        }

        if(metricBuilder.getMetrics().isEmpty()) {
            throw new IllegalArgumentException("The metric builder has no metrics!");
        }

        StringBuilder msg = new StringBuilder();

        for(Metric metric : metricBuilder.getMetrics()) {
            if(msg.length() > 0) {
                msg.append(", ");
            }
            msg.append(metric.getName() + " " + metric.getDataPoints().size() + " data points");
        }

        logger.info("Inserting data for series: " + msg.toString());

        long start = System.currentTimeMillis();
        Response response = KairosClientUtil.pushMetrics(metricBuilder, kairosRestClient);
        long end = System.currentTimeMillis();

        logger.info("Data insertion completed in " + (end - start) + "ms, response status = " + response.getStatusCode());

        if(response.getStatusCode() != 204) {
            throw new IllegalStateException("Error response " + response.getStatusCode() +
                    " when inserting data (" + msg + "), errors: " + response.getErrors());
        }
    }


    public void addTimeSeriesDataToMetricBuilder(MetricBuilder metricBuilder,
        Map<Long, Long> timeStampValueMap,
        TimeSeriesMetric timeSeriesMetric,
        Map<TrackingTag, String> tags) {

        if(metricBuilder == null) {
            throw new IllegalArgumentException("No metric builder provided");
        }

        if(timeSeriesMetric == null) {
            throw new IllegalArgumentException("No time series metric provided");
        }

        if(tags.isEmpty()) {
            throw new IllegalArgumentException("Can't assemble metric builder for metric " + timeSeriesMetric + ", no tags provided");
        }

        if(timeStampValueMap.isEmpty()) {
            throw new IllegalArgumentException("Can't assemble metric builder for metric " + timeSeriesMetric + ", no data provided");
        }

        Metric metric = metricBuilder.addMetric(timeSeriesMetric.toString());
        for(TrackingTag tag : tags.keySet()) {
            metric.addTag(tag.toString(), tags.get(tag));
        }

        for(long timeStamp : timeStampValueMap.keySet()) {
            long metricValue = timeStampValueMap.get(timeStamp);
            metric.addDataPoint(timeStamp, metricValue);
        }

        logger.info("Metric " + timeSeriesMetric + " prepped: " + timeStampValueMap.size() + " data points, tags = " + tags);
    }


    protected void adjustBucketBoundaries(Map<TimeSeriesMetric, Map<Long, Long>> metricResults,
                                       Long utcBegin, Long utcEnd, TimeUnit resultBucketSize, boolean verboseResponse) {

        for (TimeSeriesMetric timeSeriesMetric : metricResults.keySet()) {

            //
            // Adjust the bucket boundaries and inject empty buckets.
            //
            Map<Long, Long> adjustedMetricResult = adjustBoundariesForMetricResult(
                    metricResults.get(timeSeriesMetric), utcBegin, utcEnd, resultBucketSize);
            metricResults.put(timeSeriesMetric, adjustedMetricResult);

            logger.debug("Bucket adjust for time series metric " + timeSeriesMetric + ": old bucket count = " +
                    metricResults.get(timeSeriesMetric).size() + ", new bucket count = " + adjustedMetricResult.size());

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


    /**
     * When aggregating the output of a query into buckets the timestamp of the bucket is
     * unfortunately NOT equal to the start of the bucket interval. Rather the timestamp
     * of the sum in a bucket aggregated from a query equal to the first timestamp for
     * which a datapoint found in the bucket interval.
     *
     * This method adjusts the bucket starts of each bucket to align with boundaries of the
     * size indicated by the result bucket size.
     *
     * For bucket intervals for which no data was found, the result does not include a bucket.
     * This method will insert an empty bucket tied to the start of the interval for which
     * no data was found.
     *
     * Example:
     *    Database has datapoints May 29 - 3 steps, July 2nd 2 steps
     * Query:
     *    Get data starting May 1st - Aug 1
     * Result:
     *    Bucket 1 - timestamp May 29 - value/sum = 3
     *    Bucket 2 - timestamp July 2nd - value/sum = 3
     * After adjustment in this method:
     *    Bucket 1 - timestamp May 1 - value/sum = 3
     *    Bucket 2 - timestamp June 1 - value/sum = 0
     *    Bucket 2 - timestamp July 1 - value/sum = 3
     *
     * @param metricResult the result that is to have its bucket boundaries adjusted
     * @param utcBegin the start of the query interval, this also implicitly defines the
     *                 start interval for the first bucket in the result and by extension
     *                 the interval for every bucket to follow. utcBegin must be in the past.
     * @param utcEnd the end of the query interval. Can be null, in which case 'now' will be the
     *               implicit end of the query interval. If specificed, utcEnd must
     *               be in the past and AFTER utcBegin.
     * @param resultBucketSize the length of the time interval covered by each bucket.
     * @return buckets with the same values, but adjusted bucket intervals to align with
     * utcBegin and the specific result bucket size.
     */
    protected Map<Long, Long> adjustBoundariesForMetricResult(
            Map<Long, Long> metricResult, Long utcBegin, Long utcEnd, TimeUnit resultBucketSize) {

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
            //int calenderUnitToStepForward = getCalendarUnitForTimeUnit(resultBucketSize);
            //cursorCalendar.add(calenderUnitToStepForward, 1);

            long bucketEnd = BucketBoundaryUtil.getBucketEndTime(cursorCalendar.getTimeInMillis(), resultBucketSize, cursorCalendar.getTimeZone());
            cursorCalendar.setTimeInMillis(bucketEnd+1);

            if(cursorCalendar.getTimeInMillis() >= endOfInterval) {
                break;
            }
        }

        logTimeStampAdjustments(metricResult, newMetricResult);

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

    private void logTimeStampAdjustments(Map<Long, Long> metricResult, Map<Long, Long> newMetricResult) {

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




    /**
     * Create a query builder. A query builder can be passed to the kairos db client
     * to perform a time series query.
     * @param tags
     * @param timeSeriesMetric
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @return a query builder ready to be passed to the kairos db client.
     */
    protected QueryBuilder createQueryForSingleMetric(Map<TrackingTag, String> tags, TimeSeriesMetric timeSeriesMetric, Long utcBegin,
                                                    Long utcEnd, TimeUnit resultBucketSize, int resultBucketMultiplier) {

        // Do some validation of the input parameters
        QueryArgumentValidationUtil.validateQueryParameters(tags, timeSeriesMetric, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Print a description of the query in the log
        QueryLoggingUtil.logTimeSeriesQueryDescription(tags, timeSeriesMetric, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Set the time interval for the query
        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        queryBuilder.setStart(new Date(utcBegin));
        if(utcEnd != null) {
            queryBuilder.setEnd(new Date(utcEnd));
        }

        QueryMetric queryMetric = queryBuilder.addMetric(timeSeriesMetric.toString());
        queryMetric.addAggregator(AggregatorFactory.createSumAggregator(resultBucketMultiplier, resultBucketSize));
        for(TrackingTag tag : tags.keySet()) {
            queryMetric.addTag(tag.toString(), tags.get(tag));
        }

        return queryBuilder;
    }


    /**
     * Create a query builder. A query builder can be passed to the kairos db client
     * to perform a time series query.
     * @param tags
     * @param timeSeriesMetrics
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @return a query builder ready to be passed to the kairos db client.
     */
    protected QueryBuilder createQueryForMultipleMetrics(Map<TrackingTag, String> tags, List<TimeSeriesMetric> timeSeriesMetrics, Long utcBegin,
        Long utcEnd, TimeUnit resultBucketSize, int resultBucketMultiplier) {

        // Do some validation of the input parameters
        QueryArgumentValidationUtil.validateMetricsQueryParameters(tags, timeSeriesMetrics, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Print a description of the query in the log
        QueryLoggingUtil.logTimeSeriesQueryDescription(tags, timeSeriesMetrics, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Set the time interval for the query
        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        queryBuilder.setStart(new Date(utcBegin));
        if(utcEnd != null) {
            queryBuilder.setEnd(new Date(utcEnd));
        }

        for(TimeSeriesMetric timeSeriesMetric : timeSeriesMetrics) {
            QueryMetric queryMetric = queryBuilder.addMetric(timeSeriesMetric.toString());
            queryMetric.addAggregator(AggregatorFactory.createSumAggregator(resultBucketMultiplier, resultBucketSize));
            for(TrackingTag tag : tags.keySet()) {
                queryMetric.addTag(tag.toString(), tags.get(tag));
            }
        }

        return queryBuilder;
    }



    /**
     * Get the results from a query response and group the results by time series metrics
     * @param queryResponse
     * @return results from a time series query grouped by time series metric.
     */
    protected Map<TimeSeriesMetric, Map<Long, Long>> getResultFromResponse(QueryResponse queryResponse) {

        //
        // Gather the results into a data structure
        //
        // There will be one query executed for each metric that was included in the query builder
        //

        List<Queries> queries = queryResponse.getQueries();

        Map<TimeSeriesMetric, Map<Long, Long>> metricResults = new HashMap<TimeSeriesMetric, Map<Long, Long>>();

        for(Queries query : queries) {
            List<Results> results = query.getResults();
            for(Results result : results) {
                List<DataPoint> dataPoints = result.getDataPoints();
                String resultName = result.getName();
                TimeSeriesMetric timeSeriesMetric = TimeSeriesMetric.valueOf(resultName.toUpperCase());
                if(!dataPoints.isEmpty()) {
                    if (!metricResults.containsKey(resultName)) {
                        metricResults.put(timeSeriesMetric, new TreeMap<Long, Long>());
                    }
                }

                for(DataPoint dataPoint : dataPoints) {
                    metricResults.get(timeSeriesMetric).put(dataPoint.getTimestamp(), KairosClientUtil.getLongValueFromDataPoint(dataPoint));
                }
            }
        }

        return metricResults;
    }

    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        String kairosDBURL = "http://" + kairosDBHost + ":" + kairosDBPort;
        kairosRestClient = new KairosRestClient(kairosDBURL);
    }

}
