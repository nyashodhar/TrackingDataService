package com.petpal.tracking.integration;

import java.util.Map;

/**
 * Created by per on 11/3/14.
 */
public class TestTrackingData {

    private Map<Long, Long> walkingData;
    private Map<Long, Long> runningData;
    private Map<Long, Long> sleepingData;
    private Map<Long, Long> restingData;

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
        sb.append(", walkingData=").append(walkingData);
        sb.append(", runningData=").append(runningData);
        sb.append(", sleepingData=").append(sleepingData);
        sb.append(", restingData=").append(restingData);
        sb.append('}');
        return sb.toString();
    }

    public Map<Long, Long> getDataForMetric(TestTrackingMetric testTrackingMetric) {
        if (testTrackingMetric == TestTrackingMetric.WALKINGSTEPS) {
            return getWalkingData();
        } else if (testTrackingMetric == TestTrackingMetric.RUNNINGSTEPS) {
            return getRunningData();
        } else if (testTrackingMetric == TestTrackingMetric.SLEEPINGSECONDS) {
            return getSleepingData();
        } else if (testTrackingMetric == TestTrackingMetric.RESTINGSECONDS) {
            return getRestingData();
        } else {
            throw new IllegalArgumentException("Unexpected test tracking metric " + testTrackingMetric);
        }
    }

    public void setDataForMetric(TestTrackingMetric testTrackingMetric, Map<Long, Long> dataPoints) {
        if (testTrackingMetric == TestTrackingMetric.WALKINGSTEPS) {
            setWalkingData(dataPoints);
        } else if (testTrackingMetric == TestTrackingMetric.RUNNINGSTEPS) {
            setRunningData(dataPoints);
        } else if (testTrackingMetric == TestTrackingMetric.SLEEPINGSECONDS) {
            setSleepingData(dataPoints);
        } else if (testTrackingMetric == TestTrackingMetric.RESTINGSECONDS) {
            setRestingData(dataPoints);
        } else {
            throw new IllegalArgumentException("Unexpected test tracking metric " + testTrackingMetric);
        }
    }
}
