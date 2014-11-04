package com.petpal.tracking.integration;

import java.util.Map;

/**
 * Created by per on 11/3/14.
 */
public class TestTrackingData {

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
        final StringBuilder sb = new StringBuilder("TestTrackingData{");
        sb.append("trackedEntityId='").append(trackedEntityId).append('\'');
        sb.append(", trackingDeviceId='").append(trackingDeviceId).append('\'');
        sb.append(", walkingData=").append(walkingData);
        sb.append(", runningData=").append(runningData);
        sb.append(", sleepingData=").append(sleepingData);
        sb.append(", restingData=").append(restingData);
        sb.append('}');
        return sb.toString();
    }


}
