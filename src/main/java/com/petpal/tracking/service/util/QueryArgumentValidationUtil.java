package com.petpal.tracking.service.util;

import com.petpal.tracking.service.TrackingMetric;
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
                                                List<TrackingMetric> trackingMetrics,
                                                int beginUnitsIntoPast,
                                                Integer endUnitsIntoPast,
                                                TimeUnit queryIntervalTimeUnit,
                                                TimeUnit resultTimeUnit,
                                                int resultBucketMultiplier) {

        if(CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("No tags specified");
        }

        if(CollectionUtils.isEmpty(trackingMetrics)) {
            throw new IllegalArgumentException("No metrics specified");
        }

        if(!(beginUnitsIntoPast > 0)) {
            throw new IllegalArgumentException("Begin units into past must be > 0");
        }

        if(endUnitsIntoPast != null) {
            if(!(endUnitsIntoPast > 0)) {
                throw new IllegalArgumentException("Invalid value " + endUnitsIntoPast + " for end units into the past, must be > 0 if specified.");
            }
            if(!(endUnitsIntoPast < beginUnitsIntoPast)) {
                throw new IllegalArgumentException("Invalid value " + endUnitsIntoPast + " for end units into the past, must be less than <  begin units into the pastfor end units into the past.");
            }
        }

        if(endUnitsIntoPast != null && !(endUnitsIntoPast > 0)) {
            throw new IllegalArgumentException("End units into the past must be > 0 if specified.");
        }

        if(queryIntervalTimeUnit == null) {
            throw new IllegalArgumentException("Time unit type for query limiter must be specified");
        }

        if(resultTimeUnit == null) {
            throw new IllegalArgumentException("Time unit type for result bucket size must be specified");
        }

        if(!(resultBucketMultiplier > 0)) {
            throw new IllegalArgumentException("Multiplier for result bucket size must be > 0");
        }
    }


    public static void validateMetricsQueryParameters(Map<TrackingTag, String> tags,
                                                List<TrackingMetric> trackingMetrics,
                                                Long utcBegin,
                                                Long utcEnd,
                                                TimeUnit resultBucketSize,
                                                int resultBucketMultiplier) {

        if(CollectionUtils.isEmpty(tags)) {
            throw new IllegalArgumentException("No tags specified");
        }

        if(CollectionUtils.isEmpty(trackingMetrics)) {
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
