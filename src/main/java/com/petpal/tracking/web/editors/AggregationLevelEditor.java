package com.petpal.tracking.web.editors;

import com.petpal.tracking.web.controllers.AggregationLevel;

import java.beans.PropertyEditorSupport;

/**
 * Created by per on 11/24/14.
 */
public class AggregationLevelEditor extends PropertyEditorSupport {

    @Override
    public void setAsText(String text) {

        try {
            AggregationLevel aggregationLevel = AggregationLevel.valueOf(text.toUpperCase());
            this.setValue(aggregationLevel);
        } catch (Throwable t) {
            throw new IllegalArgumentException("Can't create aggregation level from string " + text);
        }
    }

    @Override
    public String getAsText() {
        AggregationLevel aggregationLevel = (AggregationLevel) this.getValue();
        return aggregationLevel.toString();
    }
}