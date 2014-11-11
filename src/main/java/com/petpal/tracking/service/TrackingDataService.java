package com.petpal.tracking.service;

import com.petpal.tracking.data.TrackingData;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by per on 10/28/14.
 */
@Component
public class TrackingDataService {

    private Logger logger = Logger.getLogger(this.getClass());

    @Autowired
    private BucketAggregationUtil bucketAggregationUtil;

    @Value("${trackingService.kairosDBHost}")
    private String kairosDBHost;

    @Value("${trackingService.kairosDBPort}")
    private String kairosDBPort;

    private KairosRestClient kairosRestClient;

    public void storeTrackingData(TrackingData trackingData) {

        System.out.println("TrackingService.storeTrackingData(): Hello - kairosDBHost=" + kairosDBHost + ", kairosDBPort=" + kairosDBPort);
        logger.info("storeTrackingData(): trackingData=" + trackingData);
        logger.info("storeTrackingData(): HELLO THERE");

        Map<TrackingTag, String> tags = new HashMap<TrackingTag, String>();
        tags.put(TrackingTag.TRACKEDENTITY, trackingData.getTrackedEntityId());
        tags.put(TrackingTag.TRACKINGDEVICE, trackingData.getTrackingDeviceId());

        MetricBuilder metricBuilder = MetricBuilder.getInstance();

        addMetricsToBuilder(metricBuilder, trackingData.getWalkingData(), TrackingMetric.WALKINGSTEPS, tags);
        addMetricsToBuilder(metricBuilder, trackingData.getRunningData(), TrackingMetric.RUNNINGSTEPS, tags);
        addMetricsToBuilder(metricBuilder, trackingData.getSleepingData(), TrackingMetric.SLEEPINGSECONDS, tags);
        addMetricsToBuilder(metricBuilder, trackingData.getRestingData(), TrackingMetric.RESTINGSECONDS, tags);

        Response response = KairosClientUtil.pushMetrics(metricBuilder, kairosRestClient);
        logger.info("Metrics pushed, response.getStatusCode() = " + response.getStatusCode());
    }


    public Map<TrackingMetric, Map<Long, Long>> getMetricsForAbsoluteRange(
            Map<TrackingTag, String> tags,
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

        logger.info("getMetricsForAbsoluteRange(): Query completed with status code " + queryResponse.getStatusCode());

        // Extract the result for response
        Map<TrackingMetric, Map<Long, Long>> metricResults = getMetricsResultFromResponse(queryResponse);

        logger.info("getMetricsForAbsoluteRange(): metricResults = " + metricResults);

        QueryLoggingUtil.printMetricsResults(metricResults);
        logger.info("getMetricsForAbsoluteRange(): Doing result adjustment..");

        // Adjust the bucket boundaries (and insert explicit empty buckets is response is to be verbose)
        adjustBucketBoundaries(metricResults, utcBegin, utcEnd, resultBucketSize, verboseResponse);

        QueryLoggingUtil.printMetricsResults(metricResults);

        return metricResults;
    }


    public Map<TrackingMetric, Map<Long, Long>> getMetrics(Map<TrackingTag, String> tags,
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
                                         TimeUnit resultBucketSize, int resultBucketMultiplier, Map<TrackingTag, String> tags) {
        for(TrackingMetric trackingMetric : trackingMetrics) {
            QueryMetric queryMetric = queryBuilder.addMetric(trackingMetric.toString());
            queryMetric.addAggregator(AggregatorFactory.createSumAggregator(resultBucketMultiplier, resultBucketSize));
            for(TrackingTag tag : tags.keySet()) {
                queryMetric.addTag(tag.toString(), tags.get(tag));
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
            Map<Long, Long> adjustedMetricResult = bucketAggregationUtil.adjustBoundariesForMetricResult(
                    metricResults.get(trackingMetric), utcBegin, utcEnd, resultBucketSize);
            metricResults.put(trackingMetric, adjustedMetricResult);

            logger.debug("Bucket adjust for metric " + trackingMetric + ": old bucket count = " +
                    metricResults.get(trackingMetric).size() + ", new bucket count = " + adjustedMetricResult.size());

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


    private void addMetricsToBuilder(MetricBuilder metricBuilder, Map<Long, Long> timeStampValueMap, TrackingMetric trackingMetric, Map<TrackingTag, String> tags) {

        if(timeStampValueMap.isEmpty()) {
            logger.info("Metric " + trackingMetric + ": no metrics provided");
            return;
        }

        Metric metric = metricBuilder.addMetric(trackingMetric.toString());
        for(TrackingTag tag : tags.keySet()) {
            metric.addTag(tag.toString(), tags.get(tag));
        }

        for(long timeStamp : timeStampValueMap.keySet()) {
            long metricValue = timeStampValueMap.get(timeStamp);
            metric.addDataPoint(timeStamp, metricValue);
        }

        logger.info("Metric " + trackingMetric + " prepped: " + timeStampValueMap.size() + " data points, tags = " + tags);
    }


    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        String kairosDBURL = "http://" + kairosDBHost + ":" + kairosDBPort;
        kairosRestClient = new KairosRestClient(kairosDBURL);
    }

}
