package com.petpal.tracking.web.validators.util;

import com.petpal.tracking.service.BucketAggregationUtil;
import com.petpal.tracking.service.timeseries.BucketBoundaryUtil;
import com.petpal.tracking.web.controllers.AggregationLevel;
import com.petpal.tracking.web.errors.InvalidControllerArgumentException;
import org.apache.log4j.Logger;

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
            AggregationLevel aggregationLevel,
            TimeZone timeZone) {

        validateAggregatedQueryParams(startYear, startMonth, startWeek, startDay, startHour, aggregationLevel);

        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.setTimeZone(timeZone);

        if(aggregationLevel == AggregationLevel.YEARS) {
            cal.set(startYear, 1, 1, 0, 0, 0);
        } else if(aggregationLevel == AggregationLevel.MONTHS) {
            cal.set(startYear, startMonth, 1, 0, 0, 0);
        } else if(aggregationLevel == AggregationLevel.WEEKS) {
            // Start at Jan 1, 00:00:00 of the specified year, then fast forward the required number of weeks
            cal.set(startYear, 1, 1, 0, 0, 0);
            cal.add(Calendar.WEEK_OF_YEAR, startWeek);
        } else if(aggregationLevel == AggregationLevel.DAYS) {
            cal.set(startYear, startMonth, startDay, 0, 0, 0);
        } else if(aggregationLevel == AggregationLevel.HOURS) {
            cal.set(startYear, startMonth, startDay, startHour, 0, 0);
        }

        return cal.getTimeInMillis();
    }


    public static Long calculateUTCEnd(Long utcBegin, AggregationLevel aggregationLevel, Integer bucketsToFetch, TimeZone timeZone) {

        if(utcBegin == null) {
            throw new InvalidControllerArgumentException("utcBegin not specified");
        }

        if(aggregationLevel == null) {
            throw new InvalidControllerArgumentException("Aggregation level not specified");
        }

        if(bucketsToFetch == null) {
            logger.info("Buckets to fetch not specified, returning null to use 'now' as default range end");
            return null;
        }

        if(bucketsToFetch.intValue() <= 0) {
            throw new InvalidControllerArgumentException("Invalid value " + bucketsToFetch + " for buckets to fetch");
        }

        Long currentBegin = utcBegin;
        Long currentEnd = null;

        for(int i=0; i<bucketsToFetch; i++) {
            currentEnd = BucketAggregationUtil.getAggregatedBucketEndTime(currentBegin, aggregationLevel, timeZone);
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
            AggregationLevel aggregationLevel) {

        if(startYear == null || startYear < 1990) {
            throw new InvalidControllerArgumentException("Invalid value " + startYear + " for start year");
        }

        if(startMonth != null) {
            if(startMonth < 0 || startMonth > 11) {
                throw new InvalidControllerArgumentException("Invalid value " + startMonth + " for start month");
            }
        }

        if(startWeek != null) {
            if(startWeek < 1 || startWeek > 52) {
                throw new InvalidControllerArgumentException("Invalid value " + startWeek + " for start week");
            }
        }

        if(startDay != null) {
            if(startDay < 1 || startDay > 31) {
                throw new InvalidControllerArgumentException("Invalid value " + startDay + " for start day");
            }
        }

        if(startHour != null) {
            if(startHour < 0 || startHour > 23) {
                throw new InvalidControllerArgumentException("Invalid value " + startHour + " for start hour");
            }
        }

        if (aggregationLevel == AggregationLevel.YEARS) {

            if(startMonth != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startMonth is specified");
            }

            if(startWeek != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startWeek is specified");
            }

            if(startDay != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startDay is specified");
            }

            if(startHour != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startHour is specified");
            }

        } else if (aggregationLevel == AggregationLevel.MONTHS) {

            if(startMonth == null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startMonth is not specified");
            }

            if(startWeek != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startWeek is specified");
            }

            if(startDay != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startDay is specified");
            }

            if(startHour != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startHour is specified");
            }

        } else if (aggregationLevel == AggregationLevel.WEEKS) {

            if(startWeek == null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but no startWeek is specified");
            }

            if(startMonth != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startMonth is specified");
            }

            if(startDay != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startDay is specified");
            }

            if(startHour != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startHour is specified");
            }

        } else if (aggregationLevel == AggregationLevel.DAYS) {

            if(startMonth == null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startMonth is not specified");
            }

            if(startWeek != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startWeek is specified");
            }

            if(startDay == null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but no startDay is specified");
            }

            if(startHour != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startHour is specified");
            }


        } else if (aggregationLevel == AggregationLevel.HOURS) {

            if(startMonth == null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startMonth is not specified");
            }

            if(startWeek != null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but startWeek is specified");
            }

            if(startDay == null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but no startDay is specified");
            }

            if(startHour == null) {
                throw new InvalidControllerArgumentException("Aggregation level is " + aggregationLevel + " but no startHour is specified");
            }
        } else {
            throw new InvalidControllerArgumentException("Invalid aggregation level " + aggregationLevel);
        }
    }

}
