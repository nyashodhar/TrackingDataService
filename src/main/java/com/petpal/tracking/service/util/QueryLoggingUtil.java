package com.petpal.tracking.service.util;

import com.petpal.tracking.service.TrackingMetric;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by per on 10/30/14.
 */
public class QueryLoggingUtil {

    private static Logger logger = Logger.getLogger(QueryLoggingUtil.class);

    public static void printMetricsResults(Map<TrackingMetric, Map<Long, Long>> metricResults) {

        for(TrackingMetric trackingMetric : metricResults.keySet()) {

            StringBuffer metricResult = new StringBuffer();
            for(Long timestamp : metricResults.get(trackingMetric).keySet()) {
                Date date = new Date();
                date.setTime(timestamp);
                if(metricResult.length() > 0) {
                    metricResult.append(", ");
                }
                //metricResult.append(date + "=" + metricResults.get(trackingMetric).get(timestamp));
                metricResult.append(getUTCFormat(date.getTime()) + "=" + metricResults.get(trackingMetric).get(timestamp));
            }
            logger.info("Result for " + trackingMetric + ": " + metricResult.toString());
        }
    }

    public static void logTimeSeriesQueryDescription(Map<String, String> tags,
                                               List<TrackingMetric> trackingMetrics,
                                               int beginUnitsIntoPast,
                                               Integer endUnitsIntoPast,
                                               TimeUnit queryIntervalTimeUnit,
                                               TimeUnit resultTimeUnit,
                                               int resultBucketMultiplier) {

        StringBuffer intervalDescriptor = new StringBuffer("[" + beginUnitsIntoPast + " " + queryIntervalTimeUnit + " ago,");
        if(endUnitsIntoPast == null) {
            intervalDescriptor.append(" now]");
        } else {
            intervalDescriptor.append(" " + endUnitsIntoPast + " " + queryIntervalTimeUnit + " ago]");
        }

        logger.info("Time series query for interval " + intervalDescriptor + " for tracking metrics " + trackingMetrics +
                ", each metric tagged by " + tags + ". Results will be grouped into buckets of " +
                resultBucketMultiplier + " " + resultTimeUnit + ".");
    }

    public static void logTimeSeriesQueryDescription(Map<String, String> tags,
                                               List<TrackingMetric> trackingMetrics,
                                               Long utcBegin,
                                               Long utcEnd,
                                               TimeUnit resultBucketSize,
                                               int resultBucketMultiplier) {

        StringBuffer intervalDescriptor = new StringBuffer("[" + getUTCFormat(utcBegin) + ",");
        if(utcEnd == null) {
            intervalDescriptor.append(" now]");
        } else {
            intervalDescriptor.append(" " + getUTCFormat(utcBegin) + "]");
        }

        logger.info("Time series query for interval " + intervalDescriptor + " for tracking metrics " + trackingMetrics +
                ", each metric tagged by " + tags + ". Results will be grouped into buckets of " +
                resultBucketMultiplier + " " + resultBucketSize + " size.");
    }

    public static String getUTCFormat(Long utcMillis) {
        if(utcMillis == null) {
            throw new IllegalArgumentException("UTCMillis was not specified");
        }
        //final Date currentTime = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE-MMM-d yyyy hh:mm:ss a z");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return simpleDateFormat.format(new Date(utcMillis.longValue()));
    }

}
