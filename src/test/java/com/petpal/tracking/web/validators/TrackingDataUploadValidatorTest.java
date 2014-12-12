package com.petpal.tracking.web.validators;

import com.petpal.tracking.service.TrackingMetricsConfig;
import com.petpal.tracking.web.controllers.TrackingDataUpload;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.Errors;

/**
 * Created by per on 12/9/14.
 */
public class TrackingDataUploadValidatorTest {

    // Class under test
    private TrackingDataUploadValidator trackingDataValidator;

    // Mocks
    private TrackingMetricsConfig trackingMetricsConfig;
    private Errors errors;

    @Before
    public void setup() {

        trackingMetricsConfig = EasyMock.createMock(TrackingMetricsConfig.class);
        errors = EasyMock.createMock(Errors.class);

        trackingDataValidator = new TrackingDataUploadValidator();
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

        TrackingDataUpload trackingDataUpload = new TrackingDataUpload();
        trackingDataValidator.validate(trackingDataUpload, errors);
        EasyMock.verify(trackingMetricsConfig, errors);
    }


}
