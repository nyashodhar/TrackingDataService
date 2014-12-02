package com.petpal.tracking.service.util;

import com.petpal.tracking.web.controllers.TrackingTag;
import com.petpal.tracking.service.TimeSeriesMetric;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by per on 10/30/14.
 */
public class QueryLoggingUtil {

    private static Logger logger = Logger.getLogger(QueryLoggingUtil.class);

    public static void printMetricsResults(Map<TimeSeriesMetric, Map<Long, Long>> metricResults) {

        for(TimeSeriesMetric timeSeriesMetric : metricResults.keySet()) {

            StringBuilder metricResult = new StringBuilder();
            for(Long timestamp : metricResults.get(timeSeriesMetric).keySet()) {
                Date date = new Date();
                date.setTime(timestamp);
                if(metricResult.length() > 0) {
                    metricResult.append(", ");
                }
                metricResult.append(getUTCFormat(date.getTime()) + "=" + metricResults.get(timeSeriesMetric).get(timestamp));
            }
            logger.debug("Result for " + timeSeriesMetric + ": " + metricResult.toString());
        }
    }


    public static void logTimeSeriesQueryDescription(Map<TrackingTag, String> tags,
                                                     TimeSeriesMetric timeSeriesMetric,
                                                     Long utcBegin,
                                                     Long utcEnd,
                                                     TimeUnit resultBucketSize,
                                                     int resultBucketMultiplier) {
        List<TimeSeriesMetric> timeSeriesMetrics = new ArrayList<TimeSeriesMetric>();
        timeSeriesMetrics.add(timeSeriesMetric);
        logTimeSeriesQueryDescription(tags, timeSeriesMetrics, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);
    }

    public static void logTimeSeriesQueryDescription(Map<TrackingTag, String> tags,
                                               List<TimeSeriesMetric> timeSeriesMetrics,
                                               Long utcBegin,
                                               Long utcEnd,
                                               TimeUnit resultBucketSize,
                                               int resultBucketMultiplier) {

        StringBuilder intervalDescriptor = new StringBuilder("[" + getUTCFormat(utcBegin) + ",");
        if(utcEnd == null) {
            intervalDescriptor.append(" now]");
        } else {
            intervalDescriptor.append(" " + getUTCFormat(utcBegin) + "]");
        }

        logger.info("Time series query for interval " + intervalDescriptor + " for time series metrics " + timeSeriesMetrics +
                ", each metric tagged by " + tags + ". Results will be grouped into buckets of " +
                resultBucketMultiplier + " " + resultBucketSize + " size.");
    }

    public static String getUTCFormat(Long utcMillis) {
        if(utcMillis == null) {
            throw new IllegalArgumentException("UTCMillis was not specified");
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE-MMM-d yyyy hh:mm:ss a z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return simpleDateFormat.format(new Date(utcMillis.longValue()));
    }

}
