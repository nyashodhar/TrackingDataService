package com.petpal.tracking.service.util;

import com.petpal.tracking.service.TrackingMetric;
import com.petpal.tracking.service.TrackingTag;
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

    public static void printMetricsResults(Map<String, Map<Long, Long>> metricResults) {

        for(String metric : metricResults.keySet()) {

            StringBuffer metricResult = new StringBuffer();
            for(Long timestamp : metricResults.get(metric).keySet()) {
                Date date = new Date();
                date.setTime(timestamp);
                if(metricResult.length() > 0) {
                    metricResult.append(", ");
                }
                metricResult.append(getUTCFormat(date.getTime()) + "=" + metricResults.get(metric).get(timestamp));
            }
            logger.info("Result for " + metric + ": " + metricResult.toString());
        }
    }

    public static void logTimeSeriesQueryDescription(Map<TrackingTag, String> tags,
                                               List<String> queryMetrics,
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

        logger.info("Time series query for interval " + intervalDescriptor + " for query metrics " + queryMetrics +
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
