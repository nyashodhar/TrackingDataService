package com.petpal.tracking.data;

import com.petpal.tracking.service.TrackingMetric;
import org.springframework.util.CollectionUtils;

import java.util.TreeMap;

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

    private TreeMap<Long, Long> walkingData;
    private TreeMap<Long, Long> runningData;
    private TreeMap<Long, Long> sleepingData;
    private TreeMap<Long, Long> restingData;

    public TreeMap<Long, Long> getWalkingData() {
        return walkingData;
    }

    public void setWalkingData(TreeMap<Long, Long> walkingData) {
        this.walkingData = walkingData;
    }

    public TreeMap<Long, Long> getRunningData() {
        return runningData;
    }

    public void setRunningData(TreeMap<Long, Long> runningData) {
        this.runningData = runningData;
    }

    public TreeMap<Long, Long> getSleepingData() {
        return sleepingData;
    }

    public void setSleepingData(TreeMap<Long, Long> sleepingData) {
        this.sleepingData = sleepingData;
    }

    public TreeMap<Long, Long> getRestingData() {
        return restingData;
    }

    public void setRestingData(TreeMap<Long, Long> restingData) {
        this.restingData = restingData;
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("TrackingData{");
        boolean comma = false;

        for(TrackingMetric trackingMetric : TrackingMetric.getAllTrackingMetrics()) {

            if(!comma) {
                comma = true;
            } else {
                sb.append(", ");
            }

            TreeMap<Long, Long> dataForMetric = getDataForMetric(trackingMetric);
            if(!CollectionUtils.isEmpty(dataForMetric)) {
                sb.append(trackingMetric + " datapoints=").append(dataForMetric.size());
            } else {
                sb.append(trackingMetric + " datapoints=0");
            }
        }

        sb.append('}');
        return sb.toString();
    }

    public TreeMap<Long, Long> getDataForMetric(TrackingMetric trackingMetric) {
        if (trackingMetric == TrackingMetric.WALKINGSTEPS) {
            return getWalkingData();
        } else if (trackingMetric == TrackingMetric.RUNNINGSTEPS) {
            return getRunningData();
        } else if (trackingMetric == TrackingMetric.SLEEPINGSECONDS) {
            return getSleepingData();
        } else if (trackingMetric == TrackingMetric.RESTINGSECONDS) {
            return getRestingData();
        } else {
            throw new IllegalArgumentException("Unexpected tracking metric " + trackingMetric);
        }
    }
}