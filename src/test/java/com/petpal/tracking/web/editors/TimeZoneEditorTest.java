package com.petpal.tracking.web.editors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.TimeZone;

/**
 * Created by per on 11/18/14.
 */
public class TimeZoneEditorTest {

    // Class under test
    private TimeZoneEditor timeZoneEditor;

    @Before
    public void setup() {
        timeZoneEditor = new TimeZoneEditor();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setAsText_null_input() {
        timeZoneEditor.setAsText(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setAsText_empty_input() {
        timeZoneEditor.setAsText("");
    }

    @Test
    public void test_setAsText_valid_input() {
        String timeZoneId = "PST";
        timeZoneEditor.setAsText(timeZoneId);
        Assert.assertEquals(timeZoneId, ((TimeZone) timeZoneEditor.getValue()).getID());
    }
}
