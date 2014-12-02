package com.petpal.tracking.web.editors;

import com.petpal.tracking.web.controllers.TrackingMetric;

import java.beans.PropertyEditorSupport;
import java.util.Arrays;
import java.util.List;

/**
 * Created by per on 10/30/14.
 */
public class TrackingMetricsSetEditor extends PropertyEditorSupport {

    //Logger logger = Logger.getLogger(this.getClass());

    @Override
    public void setAsText(String text) {

        TrackingMetricsSet trackingMetricsSet = new TrackingMetricsSet();
        List<String> trackingMetricStrings = Arrays.asList(text.split("\\s*,\\s*"));

        for(String trackingMetricString : trackingMetricStrings) {
            TrackingMetric trackingMetric = TrackingMetric.valueOf(trackingMetricString.toUpperCase());
            trackingMetricsSet.add(trackingMetric);
        }

        this.setValue(trackingMetricsSet);
    }

    @Override
    public String getAsText() {
        TrackingMetricsSet trackingMetricsSet = (TrackingMetricsSet) this.getValue();
        return trackingMetricsSet.toString();
    }
}