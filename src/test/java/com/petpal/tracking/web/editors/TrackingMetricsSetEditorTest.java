package com.petpal.tracking.web.editors;

import com.petpal.tracking.web.controllers.TrackingMetric;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by per on 10/30/14.
 */
public class TrackingMetricsSetEditorTest {

    // Class under test
    private TrackingMetricsSetEditor trackingMetricsSetEditor;

    @Before
    public void setup() {
        trackingMetricsSetEditor = new TrackingMetricsSetEditor();
    }

    @Test
    public void test_setAsText_all_valid_no_duplicate() {

        String commaSeparatedTrackingMetrics = "walkingsteps,runningsteps";

        trackingMetricsSetEditor.setAsText(commaSeparatedTrackingMetrics);

        Assert.assertTrue(trackingMetricsSetEditor.getValue() instanceof TrackingMetricsSet);

        TrackingMetricsSet trackingMetricsSet = (TrackingMetricsSet) trackingMetricsSetEditor.getValue();

        Assert.assertEquals(2, trackingMetricsSet.size());
        Assert.assertTrue(trackingMetricsSet.contains(TrackingMetric.WALKINGSTEPS));
        Assert.assertTrue(trackingMetricsSet.contains(TrackingMetric.RUNNINGSTEPS));
    }

    @Test
    public void test_setAsText_all_valid_one_duplicate() {

        String commaSeparatedTrackingMetrics = "walkingsteps,runningsteps,walkingsteps";

        trackingMetricsSetEditor.setAsText(commaSeparatedTrackingMetrics);

        Assert.assertTrue(trackingMetricsSetEditor.getValue() instanceof TrackingMetricsSet);

        TrackingMetricsSet trackingMetricsSet = (TrackingMetricsSet) trackingMetricsSetEditor.getValue();

        Assert.assertEquals(2, trackingMetricsSet.size());
        Assert.assertTrue(trackingMetricsSet.contains(TrackingMetric.WALKINGSTEPS));
        Assert.assertTrue(trackingMetricsSet.contains(TrackingMetric.RUNNINGSTEPS));
    }

}
