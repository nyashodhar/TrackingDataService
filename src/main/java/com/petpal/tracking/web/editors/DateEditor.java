package com.petpal.tracking.web.editors;

import java.beans.PropertyEditorSupport;
import java.util.Date;

/**
 * Created by per on 10/30/14.
 */
public class DateEditor extends PropertyEditorSupport {

    @Override
    public void setAsText(String text) {
        long timestamp = Long.parseLong(text);
        Date date = new Date(timestamp);
        this.setValue(date);
    }

    @Override
    public String getAsText() {
        Date date = (Date) this.getValue();
        return date.toString();
    }
}
