package com.petpal.tracking.service.util;

import com.petpal.tracking.service.TrackingTag;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by per on 10/30/14.
 */
public class QueryArgumentValidationUtil {

    public static void validateMetricsQueryParameters(Map<TrackingTag, String> tags,
                                                List<String> queryMetrics,
                                                Long utcBegin,
                                                Long utcEnd,
                                                TimeUnit resultBucketSize,
                                                int resultBucketMultiplier) {

        if(CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("No tags specified");
        }

        if(CollectionUtils.isEmpty(queryMetrics)) {
            throw new IllegalArgumentException("No metrics specified");
        }

        if(utcBegin == null) {
            throw new IllegalArgumentException("No utcBegin specified");
        }

        if(utcEnd != null) {
            //if(!(utcEnd.getTime() > utcBegin.getTime())) {
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
