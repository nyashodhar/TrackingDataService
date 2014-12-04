package com.petpal.tracking.web.validators;

import com.petpal.tracking.web.controllers.TrackingData;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

/**
 * Created by per on 12/2/14.
 */
@Component("trackingDataValidator")
public class TrackingDataValidator implements Validator {

    private static final Logger logger = Logger.getLogger(TrackingDataValidator.class);

    @Override
    public boolean supports(Class<?> paramClass) {
        return TrackingData.class.equals(paramClass);
    }

    @Override
    public void validate(Object obj, Errors errors) {

        TrackingData trackingData = (TrackingData) obj;

        //
        // TODO: Validate the tracking data
        //

        // Example on how to reject..
        /*
        Employee emp = (Employee) obj;
        if(emp.getId() <=0){
            errors.rejectValue("id", "negativeValue", new Object[]{"'id'"}, "id can't be negative");
        }

        ValidationUtils.rejectIfEmptyOrWhitespace(errors, "name", "name.required");
        ValidationUtils.rejectIfEmptyOrWhitespace(errors, "role", "role.required");
        */
    }
}
