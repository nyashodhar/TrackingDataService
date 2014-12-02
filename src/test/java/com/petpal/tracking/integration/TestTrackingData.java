package com.petpal.tracking.integration;

import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 11/3/14.
 */
public class TestTrackingData {

    private Map<TestTrackingMetric, TreeMap<Long, Long>> rawData;

    public void setData(Map<TestTrackingMetric, TreeMap<Long, Long>> rawData) {
        this.rawData = rawData;
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("TrackingData{");

        sb.append("rawData=");
        boolean comma = false;

        for(TestTrackingMetric trackingMetric : rawData.keySet()) {
            if(!comma) {
                comma = true;
            } else {
                sb.append(", ");
            }
            TreeMap<Long, Long> dataForMetric = rawData.get(trackingMetric);
            if(!CollectionUtils.isEmpty(dataForMetric)) {
                sb.append(trackingMetric + " datapoints=").append(dataForMetric.size());
            } else {
                sb.append(trackingMetric + " datapoints=0");
            }
        }
        sb.append('}');
        return sb.toString();
    }

    public TreeMap<Long, Long> getDataForMetric(TestTrackingMetric testTrackingMetric) {
       return rawData.get(testTrackingMetric);
    }

    public void setDataForMetric(TestTrackingMetric testTrackingMetric, TreeMap<Long, Long> dataPoints) {
        if(rawData == null) {
            rawData = new HashMap<TestTrackingMetric, TreeMap<Long, Long>>();
        }
        rawData.put(testTrackingMetric, dataPoints);
    }

    public Map<TestTrackingMetric, TreeMap<Long, Long>> getData() {
        return rawData;
    }
}
