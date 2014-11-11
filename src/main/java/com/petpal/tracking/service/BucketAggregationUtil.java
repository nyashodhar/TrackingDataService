package com.petpal.tracking.service;

import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.CollectionUtils;

import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Contains operations to perform calculations on buckets ranges.
 * Created by per on 11/10/14.
 */
public class BucketAggregationUtil {

    /**
     * Organize tracking data into bucket of the given time unit.
     *
     * For buckets that are larger than minute-size, the boundaries of the buckets
     * become timezone dependent.
     *
     * For example, if the client has timezone PDT, and
     * would like to see buckets grouped into
     *
     *   bucket 1: [00:00:00 may 1, 23:59:59 may 1]
     *   bucket 2: [00:00:00 may 2, 23:59:59 may 2]
     *
     * Our goal is to aggregate the input to this method in such a way that the data
     * can be stored without an aggregation being done at query time. To do that, we
     * have to aggregate the buckets as follows:
     *
     *   bucket 1: UTC timestamp for 00:00:00 may 1 in PDT timezone
     *   bucket 2: UTC timestamp for 00:00:00 may 2 in PDT timezone
     *
     * @param unaggregatedData the data to aggregate
     * @param timeZone the timezone used to calculate time ranges
     * @param bucketSize the bucket size
     *
     * @return data aggregated into the specified bucket size for the given time zone.
     */
    public TreeMap<Long, Long> aggregateIntoBucketsForTimeZone(Map<Long, Long> unaggregatedData, TimeZone timeZone, TimeUnit bucketSize) {

        if (CollectionUtils.isEmpty(unaggregatedData)) {
            return null;
        }

        if(timeZone == null) {
            throw new IllegalArgumentException("Timezone not specified for aggregation");
        }

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucket size time unit not specified for aggregation");
        }

        // Ensure the unaggregated data is sorted by timestamp

        TreeMap<Long, Long> sortedUnaggregatepData = new TreeMap<Long, Long>();
        sortedUnaggregatepData.putAll(unaggregatedData);

        long initialBucketStart = determineInitialBucket(unaggregatedData.keySet().iterator().next(), timeZone, bucketSize);

        long currentBucketStart = initialBucketStart;
        long currentBucketEnd = getBucketEndTime(currentBucketStart, bucketSize);
        TreeMap<Long, Long> aggregatedData = new TreeMap<Long, Long>();
        aggregatedData.put(currentBucketStart, 0L);

        for(Long timeStamp : sortedUnaggregatepData.keySet()) {

            long value = sortedUnaggregatepData.get(timeStamp);

            while(!(currentBucketStart <= timeStamp && currentBucketEnd >= timeStamp)) {
                //
                // The value does not belong in the current bucket, make new buckets
                // until we hit the timerange to which this value belongs
                //

                currentBucketStart = currentBucketEnd + 1L;
                currentBucketEnd = getBucketEndTime(currentBucketStart, bucketSize);
                aggregatedData.put(currentBucketStart, 0L);
            }

            // Add the value to the bucket
            long newValue = aggregatedData.get(currentBucketStart) + value;
            aggregatedData.put(currentBucketStart, newValue);
        }

        return aggregatedData;
    }


    /**
     * Determine the timestamp for the beginning of the bucket into which the data for
     * a given timestamp would be aggregated.
     * @param initialTimeStamp A UTC timestamp for which some data is to be aggregated into a bucket
     * @param timeZone the boundary point for a bucket in UTC is dependent on what timezones the client
     *                 wants to view the aggregated data relative to.
     * @param bucketSize the length of a bucket, e.g. MONTHS.
     * @return a timestamp in UTC identifying the start of the bucket into which the data for the
     * given timestamp will be aggregated.
     */
    protected long determineInitialBucket(Long initialTimeStamp, TimeZone timeZone, TimeUnit bucketSize) {

        if(initialTimeStamp == null) {
            throw new IllegalArgumentException("Initial timestamp not specified");
        }

        if(timeZone == null) {
            throw new IllegalArgumentException("Timezone not specified");
        }

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucketsize not specified");
        }

        Calendar cal = Calendar.getInstance();;
        cal.setTimeZone(timeZone);
        cal.clear();
        cal.setTimeInMillis(initialTimeStamp);

        //
        // To determine the bucket interval start, reset all the time components that are of
        // less significance than the desired bucket size time unit
        //

        Calendar bucketStartCal = Calendar.getInstance();
        bucketStartCal.clear();
        bucketStartCal.setTimeZone(timeZone);

        if (bucketSize == TimeUnit.YEARS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
        } else if (bucketSize == TimeUnit.MONTHS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
        } else if (bucketSize == TimeUnit.WEEKS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.WEEK_OF_YEAR, cal.get(Calendar.WEEK_OF_YEAR));
            bucketStartCal.set(Calendar.DAY_OF_WEEK, 1);
        } else if (bucketSize == TimeUnit.DAYS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
            bucketStartCal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH));
        } else if (bucketSize == TimeUnit.HOURS) {
            bucketStartCal.set(Calendar.YEAR, cal.get(Calendar.YEAR));
            bucketStartCal.set(Calendar.MONTH, cal.get(Calendar.MONTH));
            bucketStartCal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH));
            bucketStartCal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY));
        } else if (bucketSize == TimeUnit.MINUTES) {
            //
            // Note: TimeUnit MINUTES should not be used here. Pre-aggregation is only needed
            // applicable for timeunits larger than MINUTES.
            //
            throw new IllegalArgumentException("TimeUnit MINUTES not supported for pre-aggregration.");
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit " + bucketSize);
        }

        return bucketStartCal.getTimeInMillis();
    }


    /**
     * Calculate the end time of a bucket given its start time and bucket size
     * @param bucketStart
     * @param bucketSize
     * @return the end time of a bucket
     */
    protected long getBucketEndTime(Long bucketStart, TimeUnit bucketSize) {

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucket size not specified");
        }

        if(bucketStart == null) {
            throw new IllegalArgumentException("Bucket start not specified");
        }

        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.clear();
        cal.setTimeInMillis(bucketStart);

        int fieldToStepForward;

        if(bucketSize == TimeUnit.YEARS) {
            fieldToStepForward = Calendar.YEAR;
        } else if(bucketSize == TimeUnit.MONTHS) {
            fieldToStepForward = Calendar.MONTH;
        } else if(bucketSize == TimeUnit.WEEKS) {
            fieldToStepForward = Calendar.WEEK_OF_YEAR;
        } else if(bucketSize == TimeUnit.DAYS) {
            fieldToStepForward = Calendar.DATE;
        } else if(bucketSize == TimeUnit.HOURS) {
            fieldToStepForward = Calendar.HOUR;
        } else if(bucketSize == TimeUnit.MINUTES) {
            fieldToStepForward = Calendar.MINUTE;
        } else {
            throw new IllegalArgumentException("Unexpected TimeUnit for bucketsize: " + bucketSize);
        }

        cal.add(fieldToStepForward, 1);
        return (cal.getTimeInMillis() - 1L);
    }
}
