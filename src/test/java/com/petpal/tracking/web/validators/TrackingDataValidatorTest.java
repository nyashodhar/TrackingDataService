package com.petpal.tracking.web.validators;

import com.petpal.tracking.service.Aggregation;
import com.petpal.tracking.service.TrackingMetricConfig;
import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.web.controllers.TrackingDataTest;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.Errors;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by per on 12/9/14.
 */
public class TrackingDataValidatorTest {

    // Class under test
    private TrackingDataValidator trackingDataValidator;

    // Mocks
    private TrackingMetricsConfig trackingMetricsConfig;
    private Errors errors;

    @Before
    public void setup() {

        trackingMetricsConfig = EasyMock.createMock(TrackingMetricsConfig.class);
        errors = EasyMock.createMock(Errors.class);

        trackingDataValidator = new TrackingDataValidator();
        trackingDataValidator.setTrackingMetricsConfig(trackingMetricsConfig);
    }

    //
    // validate()
    //

    @Test
    public void testValidate_null_tracking_data() {

        errors.reject("no.tracking.data.in.body");
        EasyMock.expectLastCall();

        EasyMock.replay(trackingMetricsConfig, errors);
        trackingDataValidator.validate(null, errors);
        EasyMock.verify(trackingMetricsConfig, errors);
    }

    @Test
    public void testValidate_empty_tracking_data() {

        errors.reject("no.tracking.data.in.body");
        EasyMock.expectLastCall();

        EasyMock.replay(trackingMetricsConfig, errors);

        TrackingDataTest trackingDataTest = new TrackingDataTest();
        trackingDataValidator.validate(trackingDataTest, errors);
        EasyMock.verify(trackingMetricsConfig, errors);
    }

    //
    // validateMetricName
    //

    @Test
    public void testValidateMetricName_invalid_long_metric_name() {

        Map<String, TreeMap<Long, Long>>longMetrics = new HashMap<String, TreeMap<Long, Long>>();
        longMetrics.put("something", new TreeMap<Long, Long>());

        TrackingDataTest trackingDataTest = new TrackingDataTest();
        trackingDataTest.setLongMetrics(longMetrics);

        EasyMock.expect(trackingMetricsConfig.getTrackingMetric("something")).andThrow(new IllegalArgumentException());
        errors.reject("no.metric.config.found.for.metric.name");
        EasyMock.expectLastCall();

        EasyMock.replay(trackingMetricsConfig, errors);

        trackingDataValidator.validateMetricName(Long.class, trackingDataTest, errors);

        EasyMock.verify(trackingMetricsConfig, errors);
    }

    @Test
    public void testValidateMetricName_valid_long_metric_name() {

        Map<String, TreeMap<Long, Long>>longMetrics = new HashMap<String, TreeMap<Long, Long>>();
        longMetrics.put("something", new TreeMap<Long, Long>());

        TrackingDataTest trackingDataTest = new TrackingDataTest();
        trackingDataTest.setLongMetrics(longMetrics);

        EasyMock.expect(trackingMetricsConfig.getTrackingMetric("something")).
                andReturn(new TrackingMetricConfig("seomthing", Long.class, Aggregation.SUM));

        EasyMock.replay(trackingMetricsConfig, errors);

        trackingDataValidator.validateMetricName(Long.class, trackingDataTest, errors);

        EasyMock.verify(trackingMetricsConfig, errors);
    }
}
