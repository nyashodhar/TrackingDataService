package com.petpal.tracking.service;

import com.petpal.tracking.data.TrackingData;
import com.petpal.tracking.service.tag.TimeSeriesTag;

import java.util.Map;
import java.util.TimeZone;

/**
 * Interface used for callback from async tracking data inserter.
 * Created by per on 11/13/14.
 */
public interface AsyncTrackingDataInserter {

    /**
     * This method implements the AsyncTrackingDataInserter interface and is called asynchronously
     * when tracking data is inserted into the tracking service.
     * @param trackingData
     * @param tags
     * @param timeZone
     */
    public void asyncTrackingDataInsert(
            TrackingData trackingData, Map<TimeSeriesTag, String> tags, TimeZone timeZone);
}
