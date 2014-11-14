package com.petpal.tracking.service.async;

import com.petpal.tracking.data.TrackingData;
import com.petpal.tracking.service.AsyncTrackingDataInserter;
import com.petpal.tracking.service.tag.TimeSeriesTag;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.TimeZone;

/**
 * Created by per on 11/13/14.
 */
public class TrackingDataInsertionWorker implements Runnable {

    private static Logger logger = Logger.getLogger(TrackingDataInsertionWorker.class);

    private TrackingData trackingData;
    private Map<TimeSeriesTag, String> tags;
    private TimeZone timeZone;
    private AsyncTrackingDataInserter callBack;

    public TrackingDataInsertionWorker(AsyncTrackingDataInserter callBack, TrackingData trackingData, Map<TimeSeriesTag, String> tags, TimeZone timeZone){
        this.trackingData = trackingData;
        this.tags = tags;
        this.timeZone = timeZone;
        this.callBack = callBack;
    }

    @Override
    public void run() {

        logger.info("Tracking inserter thread starting");

        long start = System.currentTimeMillis();
        callBack.asyncTrackingDataInsert(trackingData, tags, timeZone);
        long end = System.currentTimeMillis();

        logger.info("Tracking inserter thread completed in " + (end - start) + "ms.");
    }

}