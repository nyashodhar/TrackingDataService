package com.petpal.tracking.service.timeseries;

import com.petpal.tracking.web.controllers.AggregationLevel;
import org.kairosdb.client.builder.TimeUnit;
import org.springframework.util.Assert;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by per on 11/24/14.
 */
public class BucketBoundaryUtil {

    /**
     * Calculate the end time of a bucket given its start time and bucket size
     * @param bucketStart
     * @param bucketSize
     * @param timeZone
     * @return the end time of a bucket
     */
    public static long getBucketEndTime(Long bucketStart, TimeUnit bucketSize, TimeZone timeZone) {

        Assert.notNull(bucketSize, "Bucket size not specified");
        Assert.notNull(bucketStart, "Bucket start not specified");

        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeZone(timeZone);
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
