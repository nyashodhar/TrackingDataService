package com.petpal.tracking.web.validators.util;

import com.petpal.tracking.service.util.BucketBoundaryUtil;
import org.apache.log4j.Logger;
import org.kairosdb.client.builder.TimeUnit;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * Created by per on 11/24/14.
 */
public class AggregatedQueryUtil {

    private static Logger logger = Logger.getLogger(AggregatedQueryUtil.class);

    public static Long calculateUTCBegin(
            Integer startYear,
            Integer startMonth,
            Integer startWeek,
            Integer startDay,
            Integer startHour,
            TimeUnit bucketSize,
            TimeZone timeZone) {

        validateAggregatedQueryParams(startYear, startMonth, startWeek, startDay, startHour, bucketSize);

        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeZone(timeZone);

        if(bucketSize == TimeUnit.YEARS) {
            cal.set(startYear, 1, 1, 0, 0, 0);
        } else if(bucketSize == TimeUnit.MONTHS) {
            cal.set(startYear, startMonth, 1, 0, 0, 0);
        } else if(bucketSize == TimeUnit.WEEKS) {
            // Start at Jan 1, 00:00:00 of the specified year, then fast forward the required number of weeks
            cal.set(startYear, 1, 1, 0, 0, 0);
            cal.add(Calendar.WEEK_OF_YEAR, startWeek);
        } else if(bucketSize == TimeUnit.DAYS) {
            cal.set(startYear, startMonth, startDay, 0, 0, 0);
        } else if(bucketSize == TimeUnit.HOURS) {
            cal.set(startYear, startMonth, startDay, startHour, 0, 0);
        }

        return cal.getTimeInMillis();
    }


    public static Long calculateUTCEnd(Long utcBegin, TimeUnit bucketSize, Integer bucketsToFetch, TimeZone timeZone) {

        if(utcBegin == null) {
            throw new IllegalArgumentException("utcBegin not specified");
        }

        if(bucketSize == null) {
            throw new IllegalArgumentException("Bucket size not specified");
        }

        if(bucketsToFetch == null) {
            logger.info("Buckets to fetch not specified, returning null to use 'now' as default range end");
            return null;
        }

        if(bucketsToFetch.intValue() <= 0) {
            logger.info("Invalid value " + bucketsToFetch + " for buckets to fetch, " +
                    "returning null to use 'now' as default range end");
            return null;
        }

        Long currentBegin = utcBegin;
        Long currentEnd = null;

        for(int i=0; i<bucketsToFetch; i++) {
            currentEnd = BucketBoundaryUtil.getBucketEndTime(currentBegin, bucketSize, timeZone);
            currentBegin = currentEnd + 1L;
        }

        long now = System.currentTimeMillis();
        if(currentEnd >= now) {
            logger.info("The calculated endtime (" + currentEnd + ") >= now (" +
                    now + "), returning null to use 'now' as the default range end.");
            return null;
        }

        if(currentEnd == null) {
            throw new IllegalStateException("Current end calculated as null, this should never happen");
        }

        return currentEnd;
    }



    protected static void validateAggregatedQueryParams(
            Integer startYear,
            Integer startMonth,
            Integer startWeek,
            Integer startDay,
            Integer startHour,
            TimeUnit bucketSize) {

        if(startYear < 2012 || startYear > 2100) {
            throw new IllegalArgumentException("Invalid value " + startYear + " for start year");
        }

        if(startMonth != null) {
            if(startMonth < 0 || startMonth > 11) {
                throw new IllegalArgumentException("Invalid value " + startMonth + " for start month");
            }
        }

        if(startWeek != null) {
            if(startWeek < 1 || startWeek > 52) {
                throw new IllegalArgumentException("Invalid value " + startWeek + " for start week");
            }
        }

        if(startDay != null) {
            if(startDay < 1 || startDay > 31) {
                throw new IllegalArgumentException("Invalid value " + startDay + " for start day");
            }
        }

        if(startHour != null) {
            if(startHour < 0 || startHour > 23) {
                throw new IllegalArgumentException("Invalid value " + startHour + " for start hour");
            }
        }

        if (bucketSize == TimeUnit.YEARS) {

            if(startMonth != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startMonth is specified");
            }

            if(startWeek != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startWeek is specified");
            }

            if(startDay != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startDay is specified");
            }

            if(startHour != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startHour is specified");
            }

        } else if (bucketSize == TimeUnit.MONTHS) {

            if(startMonth == null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startMonth is not specified");
            }

            if(startWeek != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startWeek is specified");
            }

            if(startDay != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startDay is specified");
            }

            if(startHour != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startHour is specified");
            }

        } else if (bucketSize == TimeUnit.WEEKS) {

            if(startWeek == null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but no startWeek is specified");
            }

            if(startMonth != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startMonth is specified");
            }

            if(startDay != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startDay is specified");
            }

            if(startHour != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startHour is specified");
            }

        } else if (bucketSize == TimeUnit.DAYS) {

            if(startMonth == null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startMonth is not specified");
            }

            if(startWeek != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startWeek is specified");
            }

            if(startDay == null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but no startDay is specified");
            }

            if(startHour != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startHour is specified");
            }


        } else if (bucketSize == TimeUnit.HOURS) {

            if(startMonth == null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startMonth is not specified");
            }

            if(startWeek != null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but startWeek is specified");
            }

            if(startDay == null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but no startDay is specified");
            }

            if(startHour == null) {
                throw new IllegalArgumentException("Bucket size is " + bucketSize + " but no startHour is specified");
            }
        } else {
            throw new IllegalArgumentException("Invalid time unit " + bucketSize);
        }
    }

}
