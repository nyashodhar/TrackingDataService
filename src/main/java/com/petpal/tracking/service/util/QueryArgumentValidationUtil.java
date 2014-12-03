package com.petpal.tracking.service.util;

import com.petpal.tracking.service.timeseries.TimeSeriesMetric;
import com.petpal.tracking.web.controllers.TrackingTag;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by per on 10/30/14.
 */
public class QueryArgumentValidationUtil {

    /*
    public static void validateQueryParameters(Map<TrackingTag, String> tags,
                                               TimeSeriesMetric timeSeriesMetric,
                                               Long utcBegin,
                                               Long utcEnd,
                                               TimeUnit resultBucketSize,
                                               int resultBucketMultiplier) {

        List<TimeSeriesMetric> timeSeriesMetrics = new ArrayList<TimeSeriesMetric>();
        timeSeriesMetrics.add(timeSeriesMetric);
        validateMetricsQueryParameters(tags, timeSeriesMetrics, utcBegin, utcEnd, resultBucketSize, resultBucketMultiplier);
    }

    public static void validateMetricsQueryParameters(Map<TrackingTag, String> tags,
                                                      List<TimeSeriesMetric> timeSeriesMetrics,
                                                      Long utcBegin,
                                                      Long utcEnd,
                                                      TimeUnit resultBucketSize,
                                                      int resultBucketMultiplier) {

        Assert.notEmpty(tags, "No tags specified");
        Assert.notEmpty(timeSeriesMetrics, "No time series metrics specified");
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
    */
}
