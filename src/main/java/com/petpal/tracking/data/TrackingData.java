package com.petpal.tracking.data;

import com.petpal.tracking.service.TrackingMetric;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by per on 10/28/14.
 */
public class TrackingData {

    //
    // Map<long timestamp, long walkingsteps> walkingData
    // Map<long timestamp, long runningsteps> runningData
    //
    // Map<long timestamp, long seconds> sleepData
    // Map<long timestamp, long seconds> restData
    //
    // CURL EXAMPLE:
    // curl -v -X POST localhost:9000/tracking -H "Accept: application/json" -H "Content-Type: application/json" -d '{"walkingData":{"23423424523523":123,"23423424523700":125}}'
    //

    private String trackedEntityId;
    private String trackingDeviceId;

    private Map<Long, Long> walkingData;
    private Map<Long, Long> runningData;
    private Map<Long, Long> sleepingData;
    private Map<Long, Long> restingData;

    public String getTrackedEntityId() {
        return trackedEntityId;
    }

    public void setTrackedEntityId(String trackedEntityId) {
        this.trackedEntityId = trackedEntityId;
    }

    public String getTrackingDeviceId() {
        return trackingDeviceId;
    }

    public void setTrackingDeviceId(String trackingDeviceId) {
        this.trackingDeviceId = trackingDeviceId;
    }

    public Map<Long, Long> getWalkingData() {
        return walkingData;
    }

    public void setWalkingData(Map<Long, Long> walkingData) {
        this.walkingData = walkingData;
    }

    public Map<Long, Long> getRunningData() {
        return runningData;
    }

    public void setRunningData(Map<Long, Long> runningData) {
        this.runningData = runningData;
    }

    public Map<Long, Long> getSleepingData() {
        return sleepingData;
    }

    public void setSleepingData(Map<Long, Long> sleepingData) {
        this.sleepingData = sleepingData;
    }

    public Map<Long, Long> getRestingData() {
        return restingData;
    }

    public void setRestingData(Map<Long, Long> restingData) {
        this.restingData = restingData;
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("TrackingData{");

        sb.append("trackedEntityId='").append(trackedEntityId).append('\'');
        sb.append(", trackingDeviceId='").append(trackingDeviceId).append('\'');

        if(CollectionUtils.isEmpty(getWalkingData())) {
            sb.append(", walkingDataPoints=").append(walkingData.size());
        }

        if(CollectionUtils.isEmpty(getRunningData())) {
            sb.append(", runningDataPoints=").append(runningData.size());
        }

        if(CollectionUtils.isEmpty(getSleepingData())) {
            sb.append(", sleepingDataPoints=").append(sleepingData.size());
        }

        if(CollectionUtils.isEmpty(getRestingData())) {
            sb.append(", restingDataPoints=").append(restingData.size());
        }

        sb.append('}');
        return sb.toString();
    }

    public Map<Long, Long> getDataForMetric(TrackingMetric testTrackingMetric) {
        if (testTrackingMetric == TrackingMetric.WALKINGSTEPS) {
            return getWalkingData();
        } else if (testTrackingMetric == TrackingMetric.RUNNINGSTEPS) {
            return getRunningData();
        } else if (testTrackingMetric == TrackingMetric.SLEEPINGSECONDS) {
            return getSleepingData();
        } else if (testTrackingMetric == TrackingMetric.RESTINGSECONDS) {
            return getRestingData();
        } else {
            throw new IllegalArgumentException("Unexpected tracking metric " + testTrackingMetric);
        }
    }

    public void setDataForMetric(TrackingMetric testTrackingMetric, Map<Long, Long> dataPoints) {
        if (testTrackingMetric == TrackingMetric.WALKINGSTEPS) {
            setWalkingData(dataPoints);
        } else if (testTrackingMetric == TrackingMetric.RUNNINGSTEPS) {
            setRunningData(dataPoints);
        } else if (testTrackingMetric == TrackingMetric.SLEEPINGSECONDS) {
            setSleepingData(dataPoints);
        } else if (testTrackingMetric == TrackingMetric.RESTINGSECONDS) {
            setRestingData(dataPoints);
        } else {
            throw new IllegalArgumentException("Unexpected tracking metric " + testTrackingMetric);
        }
    }

}


