package com.petpal.tracking.service.util;

import com.petpal.tracking.web.controllers.TrackingTag;
import com.petpal.tracking.service.TimeSeriesMetric;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by per on 10/30/14.
 */
public class QueryArgumentValidationUtil {

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

        if (CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("No tags specified");
        }

        if (CollectionUtils.isEmpty(timeSeriesMetrics)) {
            throw new IllegalArgumentException("No time series metrics specified");
        }

        if (utcBegin == null) {
            throw new IllegalArgumentException("No utcBegin specified");
        }

        if (utcEnd != null) {
            if (!(utcEnd.longValue() > utcBegin)) {
                throw new IllegalArgumentException("The time stamp " + utcEnd + " for end of time interval, must be " +
                        "> than the timestamp for the start of the interval (" + utcBegin + ")");
            }
        }

        if (resultBucketSize == null) {
            throw new IllegalArgumentException("Time unit type for result bucket size must be specified");
        }

        if (!(resultBucketMultiplier > 0)) {
            throw new IllegalArgumentException("Multiplier for result bucket size must be > 0");
        }
    }
}
