package com.petpal.tracking.data;

import com.petpal.tracking.service.TrackingMetric;
import org.springframework.util.CollectionUtils;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 10/28/14.
 */
public class TrackingData {

    private Map<TrackingMetric, TreeMap<Long, Long>> rawData;

    public void setData(Map<TrackingMetric, TreeMap<Long, Long>> rawData) {
        this.rawData = rawData;
    }

    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder("TrackingData{");

        sb.append("rawData=");
        boolean comma = false;

        for(TrackingMetric trackingMetric : rawData.keySet()) {
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

    public TreeMap<Long, Long> getDataForMetric(TrackingMetric trackingMetric) {
        return rawData.get(trackingMetric);
    }

    public Map<TrackingMetric, TreeMap<Long, Long>> getRawData() {
        return rawData;
    }
}