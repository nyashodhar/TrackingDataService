package com.petpal.tracking.web.editors;

import org.kairosdb.client.builder.TimeUnit;

import java.beans.PropertyEditorSupport;

/**
 * Created by per on 11/24/14.
 */
public class TimeUnitEditor extends PropertyEditorSupport {

    @Override
    public void setAsText(String text) {

        try {
            TimeUnit timeUnit = TimeUnit.valueOf(text.toUpperCase());
            this.setValue(timeUnit);
        } catch (Throwable t) {
            throw new IllegalArgumentException("Can't create TimeUnit from string " + text);
        }
    }

    @Override
    public String getAsText() {
        TimeUnit timeUnit = (TimeUnit) this.getValue();
        return timeUnit.toString();
    }
}