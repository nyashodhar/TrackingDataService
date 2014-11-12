package com.petpal.tracking.service.util;

import com.petpal.tracking.service.metrics.TimeSeriesMetric;
import com.petpal.tracking.service.tag.TimeSeriesTag;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by per on 10/30/14.
 */
public class QueryArgumentValidationUtil {

    public static void validateMetricsQueryParameters(Map<TimeSeriesTag, String> tags,
                                                List<TimeSeriesMetric> timeSeriesMetrics,
                                                Long utcBegin,
                                                Long utcEnd,
                                                TimeUnit resultBucketSize,
                                                int resultBucketMultiplier) {

        if(CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("No tags specified");
        }

        if(CollectionUtils.isEmpty(timeSeriesMetrics)) {
            throw new IllegalArgumentException("No time series metrics specified");
        }

        if(utcBegin == null) {
            throw new IllegalArgumentException("No utcBegin specified");
        }

        if(utcEnd != null) {
            if(!(utcEnd.longValue() > utcBegin)) {
                throw new IllegalArgumentException("The time stamp " + utcEnd + " for end of time interval, must be " +
                        "> than the timestamp for the start of the interval (" + utcBegin + ")");
            }
        }

        if(resultBucketSize == null) {
            throw new IllegalArgumentException("Time unit type for result bucket size must be specified");
        }

        if(!(resultBucketMultiplier > 0)) {
            throw new IllegalArgumentException("Multiplier for result bucket size must be > 0");
        }
    }


}
