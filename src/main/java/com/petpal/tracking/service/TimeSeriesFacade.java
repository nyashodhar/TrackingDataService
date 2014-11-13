package com.petpal.tracking.service;

import com.petpal.tracking.service.metrics.TimeSeriesMetric;
import com.petpal.tracking.service.tag.TimeSeriesTag;
import com.petpal.tracking.service.util.QueryArgumentValidationUtil;
import com.petpal.tracking.service.util.QueryLoggingUtil;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            Map<TimeSeriesTag, String> tags,
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
        logger.info("querySingleTimeSeries(): Query completed with status code " + queryResponse.getStatusCode());

        // Extract the result for response
        Map<TimeSeriesMetric, Map<Long, Long>> resultsByMetric = getResultFromResponse(queryResponse);
        QueryLoggingUtil.printMetricsResults(resultsByMetric);

        // Adjust the bucket boundaries (and insert explicit empty buckets is response is to be verbose)
        logger.info("querySingleTimeSeries(): Doing result adjustment..");
        bucketAggregationUtil.adjustBucketBoundaries(resultsByMetric, utcBegin, utcEnd, resultBucketSize, verboseResponse);
        QueryLoggingUtil.printMetricsResults(resultsByMetric);

        Map<Long, Long> results = resultsByMetric.get(timeSeriesMetric);
        logger.info("querySingleTimeSeries(): results = " + results);

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
            Map<TimeSeriesTag, String> tags,
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
        bucketAggregationUtil.adjustBucketBoundaries(resultsByMetric, utcBegin, utcEnd, resultBucketSize, verboseResponse);
        QueryLoggingUtil.printMetricsResults(resultsByMetric);

        return resultsByMetric;
    }


    /**
     * Persists data for a time series
     * @param timeStampValueMap
     * @param timeSeriesMetric
     * @param tags
     * @return a metric builder with data than can be passed to the kairos db
     * client to be persisted.
     */
    public void insertDataForSeries(Map<Long, Long> timeStampValueMap, TimeSeriesMetric timeSeriesMetric, Map<TimeSeriesTag, String> tags) {

        if(timeSeriesMetric == null) {
            throw new IllegalArgumentException("No time series metric provided");
        }

        if(tags.isEmpty()) {
            throw new IllegalArgumentException("Can't assemble metric builder for metric " + timeSeriesMetric + ", no tags provided");
        }

        if(timeStampValueMap.isEmpty()) {
            throw new IllegalArgumentException("Can't assemble metric builder for metric " + timeSeriesMetric + ", no data provided");
        }

        MetricBuilder metricBuilder = MetricBuilder.getInstance();

        Metric metric = metricBuilder.addMetric(timeSeriesMetric.toString());
        for(TimeSeriesTag tag : tags.keySet()) {
            metric.addTag(tag.toString(), tags.get(tag));
        }

        for(long timeStamp : timeStampValueMap.keySet()) {
            long metricValue = timeStampValueMap.get(timeStamp);
            metric.addDataPoint(timeStamp, metricValue);
        }

        logger.info("MetricBuilder prepared for time series metric " + timeSeriesMetric + ": " + timeStampValueMap.size() + " data points, tags = " + tags);

        Response response = KairosClientUtil.pushMetrics(metricBuilder, kairosRestClient);
        logger.info("Data inserted for series " + timeSeriesMetric + ", data points = " +
                timeStampValueMap.size() + ", response.getStatusCode() = " + response.getStatusCode());

        if(response.getStatusCode() != 204) {
            throw new IllegalStateException("Error response " + response.getStatusCode() +
                    " when inserting data for series " + timeSeriesMetric + ", errors: " + response.getErrors());
        }
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
    protected QueryBuilder createQueryForSingleMetric(Map<TimeSeriesTag, String> tags, TimeSeriesMetric timeSeriesMetric, Long utcBegin,
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
        for(TimeSeriesTag tag : tags.keySet()) {
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
    protected QueryBuilder createQueryForMultipleMetrics(Map<TimeSeriesTag, String> tags, List<TimeSeriesMetric> timeSeriesMetrics, Long utcBegin,
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
            for(TimeSeriesTag tag : tags.keySet()) {
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
