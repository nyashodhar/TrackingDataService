package com.petpal.tracking.web.editors;

import java.beans.PropertyEditorSupport;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

/**
 * Created by per on 11/18/14.
 */
public class TimeZoneEditor extends PropertyEditorSupport {

    private static final Set<String> validTimeZoneIds =
            new HashSet<String>(Arrays.asList(TimeZone.getAvailableIDs()));

    @Override
    public void setAsText(String text) {

        if(text == null) {
            throw new IllegalArgumentException("No timezone ID argument given");
        }

        if(!validTimeZoneIds.contains(text)) {
            throw new IllegalArgumentException("The arg '" + text + "' is not a valid timezone id");
        }
        this.setValue(TimeZone.getTimeZone(text));
    }

    @Override
    public String getAsText() {
        TimeZone timeZone = (TimeZone) this.getValue();
        return timeZone.toString();
    }
}