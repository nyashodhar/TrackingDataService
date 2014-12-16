package com.petpal.tracking.service.timeseries;

import com.petpal.tracking.service.TrackingMetricConfig;
import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.web.controllers.TrackingData;
import com.petpal.tracking.web.controllers.TrackingTag;
import org.apache.commons.lang.math.LongRange;
import org.apache.log4j.Logger;
import org.kairosdb.client.KairosClientUtil;
import org.kairosdb.client.KairosRestClient;
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
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
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
@Component("timeSeriesFacade")
public class TimeSeriesFacadeImpl implements TimeSeriesFacade {

    private Logger logger = Logger.getLogger(this.getClass());

    @Value("${trackingService.kairosDBHost}")
    private String kairosDBHost;

    @Value("${trackingService.kairosDBPort}")
    private String kairosDBPort;

    private KairosRestClient kairosRestClient;

    @Autowired
    private TrackingMetricsConfig trackingMetricsConfig;

    /**
     * @see com.petpal.tracking.service.timeseries.TimeSeriesFacade#querySingleTimeSeries(java.util.Map, String, java.lang.reflect.Type, Long, Long, org.kairosdb.client.builder.TimeUnit, int, boolean)
     */
    public TreeMap querySingleTimeSeries(
            Map<TrackingTag, String> tags,
            String timeSeriesName,
            Type dataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean performBucketAdjustment) {

        // Build the query
        QueryBuilder queryBuilder = createQueryForSingleMetric(
                tags, timeSeriesName, dataType, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Do the query
        QueryResponse queryResponse = KairosClientUtil.executeQuery(queryBuilder, kairosRestClient);
        logger.debug("querySingleTimeSeries(): Query completed with status code " +
                queryResponse.getStatusCode() + " for series " + timeSeriesName);

        // Extract the result for response

        Map<String, Type> timeSeriesNameToDataType = new HashMap<String, Type>();
        timeSeriesNameToDataType.put(timeSeriesName, dataType);

        Map<String, TreeMap> resultsByMetric = getResultFromResponse(queryResponse, timeSeriesNameToDataType);
        printMetricsResults(resultsByMetric);

        // Adjust the bucket boundaries (and insert explicit empty buckets is response is to be verbose)
        logger.debug("querySingleTimeSeries(): Doing result adjustment for series " + timeSeriesName);

        if(performBucketAdjustment) {
            adjustBucketBoundaries(resultsByMetric, utcBegin, utcEnd, resultBucketSize, timeSeriesNameToDataType);
        }

        printMetricsResults(resultsByMetric);

        TreeMap results = resultsByMetric.get(timeSeriesName);
        logger.debug("querySingleTimeSeries(): results = " + results);

        return results;
    }

    /**
     * @see com.petpal.tracking.service.timeseries.TimeSeriesFacade#queryMultipleTimeSeries(java.util.Map, java.util.Map, Long, Long, org.kairosdb.client.builder.TimeUnit, int, boolean)
     */
    public Map<String, TreeMap> queryMultipleTimeSeries(
            Map<TrackingTag, String> tags,
            Map<String, Type> timeSeriesNamesToDataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier,
            boolean performBucketAdjustment) {

        // Build the query
        QueryBuilder queryBuilder = createQueryForMultipleMetrics(
                tags, timeSeriesNamesToDataType, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Do the query
        QueryResponse queryResponse = KairosClientUtil.executeQuery(queryBuilder, kairosRestClient);
        logger.info("queryMultipleTimeSeries(): Query completed with status code " + queryResponse.getStatusCode());

        // Extract the result for response
        Map<String, TreeMap> resultsByTimeSeriesName = getResultFromResponse(queryResponse, timeSeriesNamesToDataType);

        printMetricsResults(resultsByTimeSeriesName);


        // Adjust the bucket boundaries (and insert explicit empty buckets is response is to be verbose)
        logger.info("queryMultipleTimeSeries(): Doing result adjustment..");

        if(performBucketAdjustment) {
            adjustBucketBoundaries(resultsByTimeSeriesName, utcBegin, utcEnd, resultBucketSize, timeSeriesNamesToDataType);
        }

        printMetricsResults(resultsByTimeSeriesName);

        return resultsByTimeSeriesName;
    }


    /**
     * @see com.petpal.tracking.service.timeseries.TimeSeriesFacade#storeDataForTimeSeries(java.util.TreeMap, String, java.lang.reflect.Type, java.util.Map)
     */
    public void storeDataForTimeSeries(TreeMap dataPoints, String timeSeriesName, Type dataType, Map<TrackingTag, String> tags) {
        MetricBuilder metricBuilder = MetricBuilder.getInstance();
        addTimeSeriesDataToMetricBuilder(metricBuilder, dataPoints, dataType, timeSeriesName, tags);
        insertData(metricBuilder);
    }

    /**
     * @see com.petpal.tracking.service.timeseries.TimeSeriesFacade#storeRawMetrics(com.petpal.tracking.web.controllers.TrackingData, java.util.Map)
     */
    public void storeRawMetrics(TrackingData trackingData, Map<TrackingTag, String> tags) {

        MetricBuilder metricBuilder = MetricBuilder.getInstance();

        Map<TrackingMetricConfig, TreeMap> metricConfigAndData = trackingData.getMetricConfigAndData();

        for(TrackingMetricConfig trackingMetricConfig : metricConfigAndData.keySet()) {

            TreeMap dataPoints = metricConfigAndData.get(trackingMetricConfig);
            String timeSeriesName = trackingMetricConfig.getUnaggregatedSeriesName();

            if(!CollectionUtils.isEmpty(dataPoints)) {
                addTimeSeriesDataToMetricBuilder(metricBuilder, dataPoints, trackingMetricConfig.getDataType(), timeSeriesName, tags);
            } else {
                logger.info("No datapoints found for " + trackingMetricConfig.getDataType() + " metric " +
                        trackingMetricConfig.getName() + ", skipping update of time series " + timeSeriesName);
            }
        }

        insertData(metricBuilder);
    }

    /**
     * Persists data for a time series
     * @param metricBuilder
     * @return a metric builder with data than can be passed to the kairos db
     * client to be persisted.
     */
    protected void insertData(MetricBuilder metricBuilder) {

        Assert.notNull(metricBuilder, "No metric builder provided");
        Assert.notEmpty(metricBuilder.getMetrics(), "The metric builder has no metrics!");

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


    protected void addTimeSeriesDataToMetricBuilder(
            MetricBuilder metricBuilder,
            TreeMap dataPoints,
            Type dataType,
            String timeSeriesName,
            Map<TrackingTag, String> tags) {

        Assert.isTrue(dataType == Long.class || dataType == Double.class || dataType == String.class, "Invalid data type " + dataType);
        Assert.notNull(metricBuilder, "No metric builder provided");
        Assert.isTrue(!StringUtils.isEmpty(timeSeriesName), "No time series name provided");
        Assert.notEmpty(tags, "Can't assemble metric builder for " + dataType + " time series " + timeSeriesName + ", no tags provided");
        Assert.notEmpty(dataPoints, "Can't assemble metric builder for " + dataType + " time series " + timeSeriesName + ", no data points provided");

        Metric metric = metricBuilder.addMetric(timeSeriesName);
        for(TrackingTag tag : tags.keySet()) {
            metric.addTag(tag.toString(), tags.get(tag));
        }

        for(Object timeStampObj : dataPoints.keySet()) {
            Object metricValue = dataPoints.get(timeStampObj);
            metric.addDataPoint((Long) timeStampObj, metricValue);
        }

        logger.info("Metrics for time series " + timeSeriesName + " prepped: " + dataPoints.size() + " data points, tags = " + tags);
    }


    /**************************************
     *
     * THIS METHOD IS NOT SAFE FOR USE WITH PRE-AGGRAGATED SERIES.
     * WHEN INJECTING NEW BUCKETS INTO THOSE SERIES, IT WILL NOT
     * CREATE CORRECT BOUNDARIES.
     *
     * THE 'verbose response feature' should be REMOVED. It's worthless.
     ***************************************/
    protected void adjustBucketBoundaries(
            Map<String, TreeMap> metricResults,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            Map<String, Type> timeSeriesNamesToDataType) {

        for(String timeSeriesName : metricResults.keySet()) {

            Type dataType = timeSeriesNamesToDataType.get(timeSeriesName);
            if (dataType != Long.class) {
                throw new IllegalArgumentException("adjustBucketBoundaries(): Only Long type supported");
            }

            //
            // Adjust the bucket boundaries and inject empty buckets.
            //
            TreeMap adjustedMetricResult = adjustBoundariesForMetricResult(
                    metricResults.get(timeSeriesName), utcBegin, utcEnd, resultBucketSize);
            metricResults.put(timeSeriesName, adjustedMetricResult);

            logger.debug("Bucket adjust for time series " + timeSeriesName + ": old bucket count = " +
                    metricResults.get(timeSeriesName).size() + ", new bucket count = " + adjustedMetricResult.size());

            // Filter out the empty buckets.
            Set<Long> bucketTimeStamps = new HashSet<Long>(adjustedMetricResult.keySet());
            for (Long timestamp : bucketTimeStamps) {
                if (((Long) adjustedMetricResult.get(timestamp)).longValue() == 0L) {
                    adjustedMetricResult.remove(timestamp);
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
    protected TreeMap adjustBoundariesForMetricResult(
            TreeMap metricResult, Long utcBegin, Long utcEnd, TimeUnit resultBucketSize) {

        Assert.notNull(utcBegin, "utcBegin not specified");

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

        TreeMap newMetricResult = new TreeMap();

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

        for(Object oldTimeStampObj : metricResult.keySet()) {
            Long oldTimeStamp = (Long) oldTimeStampObj;
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
            oldTimeStamps.append(getUTCFormat(oldTimeStamp) + "(" + oldTimeStamp  + ")");
        }

        logger.debug("Old bucket boundaries: [" + oldTimeStamps + "]");

        StringBuilder newTimeStamps = new StringBuilder();
        for(long newTimeStamp : newMetricResult.keySet()) {
            if(newTimeStamps.length() > 0) {
                newTimeStamps.append(", ");
            }
            newTimeStamps.append(getUTCFormat(newTimeStamp) + "(" + newTimeStamp  + ")");
        }

        logger.debug("New bucket boundaries: [" + newTimeStamps + "]");
    }


    /**
     * Create a query builder. A query builder can be passed to the kairos db client
     * to perform a time series query.
     * @param tags
     * @param timeSeriesName
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @return a query builder ready to be passed to the kairos db client.
     */
    protected QueryBuilder createQueryForSingleMetric(
            Map<TrackingTag, String> tags,
            String timeSeriesName,
            Type dataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier) {

        // Do some validation of the input parameters
        validateQueryParameters(tags, timeSeriesName, dataType, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Print a description of the query in the log

        Map<String, Type> timeSeriesToDataType = new HashMap<String, Type>();
        timeSeriesToDataType.put(timeSeriesName, dataType);
        logTimeSeriesQueryDescription(tags, timeSeriesToDataType, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Set the time interval for the query
        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        queryBuilder.setStart(new Date(utcBegin));
        if(utcEnd != null) {
            queryBuilder.setEnd(new Date(utcEnd));
        }

        QueryMetric queryMetric = queryBuilder.addMetric(timeSeriesName);
        for(TrackingTag tag : tags.keySet()) {
            queryMetric.addTag(tag.toString(), tags.get(tag));
        }

        return queryBuilder;
    }


    /**
     * Create a query builder. A query builder can be passed to the kairos db client
     * to perform a time series query.
     * @param tags
     * @param timeSeriesNamesToDataType
     * @param utcBegin
     * @param utcEnd
     * @param resultBucketSize
     * @param resultBucketMultiplier
     * @return a query builder ready to be passed to the kairos db client.
     */
    protected QueryBuilder createQueryForMultipleMetrics(
            Map<TrackingTag,
            String> tags,
            Map<String, Type> timeSeriesNamesToDataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier) {

        // Do some validation of the input parameters
        validateMetricsQueryParameters(tags, timeSeriesNamesToDataType, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Print a description of the query in the log
        logTimeSeriesQueryDescription(tags, timeSeriesNamesToDataType, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);

        // Set the time interval for the query
        QueryBuilder queryBuilder = QueryBuilder.getInstance();
        queryBuilder.setStart(new Date(utcBegin));
        if(utcEnd != null) {
            queryBuilder.setEnd(new Date(utcEnd));
        }

        for(String timeSeriesName : timeSeriesNamesToDataType.keySet()) {
            QueryMetric queryMetric = queryBuilder.addMetric(timeSeriesName);
            for(TrackingTag tag : tags.keySet()) {
                queryMetric.addTag(tag.toString(), tags.get(tag));
            }
        }

        return queryBuilder;
    }



    /**
     * Get the results from a query response and group the results by time series metrics
     * @param queryResponse
     * @param timeSeriesNamesToDataType
     * @return results from a time series query grouped by time series metric.
     */
    protected Map<String, TreeMap> getResultFromResponse(
            QueryResponse queryResponse, Map<String, Type> timeSeriesNamesToDataType) {

        //
        // Gather the results into a data structure
        //
        // There will be one query executed for each metric that was included in the query builder
        //

        List<Queries> queries = queryResponse.getQueries();

        Map<String, TreeMap> metricResults = new HashMap<String, TreeMap>();

        for(Queries query : queries) {
            List<Results> results = query.getResults();
            for(Results result : results) {
                List<DataPoint> dataPoints = result.getDataPoints();
                String timeSeriesName = result.getName();
                if(!dataPoints.isEmpty()) {
                    if (!metricResults.containsKey(timeSeriesName)) {
                        metricResults.put(timeSeriesName, new TreeMap());
                    }
                }

                Type dataType = timeSeriesNamesToDataType.get(timeSeriesName);
                if(dataType == null) {
                    throw new IllegalStateException("The query returned result for an unexpected time series " + timeSeriesName);
                }

                for(DataPoint dataPoint : dataPoints) {

                    Object value;

                    if(dataType != Long.class) {
                        throw new IllegalArgumentException("getResultFromResponse(): Only Long data type supported");
                    } else {
                        value = KairosClientUtil.getLongValueFromDataPoint(dataPoint);
                    }

                    metricResults.get(timeSeriesName).put(dataPoint.getTimestamp(), value);
                }
            }
        }

        return metricResults;
    }


    private void validateQueryParameters(
            Map<TrackingTag, String> tags,
            String timeSeriesName,
            Type dataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier) {

        List<String> timeSeriesNames = new ArrayList<String>();
        timeSeriesNames.add(timeSeriesName);

        Assert.notNull(dataType, "No datatype specified");

        Map<String, Type> timeSeriesNameToDataType = new HashMap<String, Type>();
        timeSeriesNameToDataType.put(timeSeriesName, dataType);
        validateMetricsQueryParameters(tags, timeSeriesNameToDataType, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);
    }

    private void validateMetricsQueryParameters(
            Map<TrackingTag, String> tags,
            Map<String, Type> timeSeriesNamesToDataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier) {

        Assert.notEmpty(tags, "No tags specified");
        Assert.notEmpty(timeSeriesNamesToDataType, "No time series name and type mapping specified");
        Assert.notNull(utcBegin, "No utcBegin specified");

        if (utcEnd != null) {
            if (!(utcEnd.longValue() > utcBegin)) {
                throw new IllegalArgumentException("The time stamp " + utcEnd + " for end of time interval, must be " +
                        "> than the timestamp for the start of the interval (" + utcBegin + ")");
            }
        }

        Assert.notNull(resultBucketSize, "Time unit type for result bucket size must be specified");

        if (!(resultBucketMultiplier > 0)) {
            throw new IllegalArgumentException("Multiplier for result bucket size must be > 0");
        }
    }

    private void printMetricsResults(Map<String, TreeMap> metricResults) {

        for(String timeSeriesName : metricResults.keySet()) {

            StringBuilder metricResult = new StringBuilder();
            for(Object timestampObj : metricResults.get(timeSeriesName).keySet()) {
                Long timestamp = (Long) timestampObj;
                Date date = new Date();
                date.setTime(timestamp);
                if(metricResult.length() > 0) {
                    metricResult.append(", ");
                }
                metricResult.append(getUTCFormat(date.getTime()) + "=" + metricResults.get(timeSeriesName).get(timestamp));
            }
            logger.debug("Result for " + timeSeriesName + ": " + metricResult.toString());
        }
    }

    private String getUTCFormat(Long utcMillis) {
        if(utcMillis == null) {
            throw new IllegalArgumentException("UTCMillis was not specified");
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE-MMM-d yyyy hh:mm:ss a z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return simpleDateFormat.format(new Date(utcMillis.longValue()));
    }

    private void logTimeSeriesQueryDescription(
            Map<TrackingTag, String> tags,
            Map<String, Type> timeSeriesNamesToDataType,
            Long utcBegin,
            Long utcEnd,
            TimeUnit resultBucketSize,
            int resultBucketMultiplier) {

        StringBuilder intervalDescriptor = new StringBuilder("[" + getUTCFormat(utcBegin) + ",");
        if(utcEnd == null) {
            intervalDescriptor.append(" now]");
        } else {
            intervalDescriptor.append(" " + getUTCFormat(utcEnd) + "]");
        }

        logger.info("Time series query for interval " + intervalDescriptor + " for time series " + timeSeriesNamesToDataType +
                ", each metric series tagged by " + tags + ". Results will be grouped into buckets of " +
                resultBucketMultiplier + " " + resultBucketSize + " size.");
    }

    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        String kairosDBURL = "http://" + kairosDBHost + ":" + kairosDBPort;
        kairosRestClient = new KairosRestClient(kairosDBURL);
    }
}
