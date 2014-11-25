package com.petpal.tracking.web.editors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kairosdb.client.builder.TimeUnit;

/**
 * Created by per on 11/24/14.
 */
public class TimeUnitEditorTest {

    // Class under test
    private TimeUnitEditor timeUnitEditor;

    @Before
    public void setup() {
        timeUnitEditor = new TimeUnitEditor();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setAsText_null() {
        timeUnitEditor.setAsText(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setAsText_invalid() {
        timeUnitEditor.setAsText("something");
    }

    @Test
    public void test_setAsText_valid() {
        timeUnitEditor.setAsText("YearS");
        Assert.assertEquals(TimeUnit.YEARS, timeUnitEditor.getValue());

        timeUnitEditor.setAsText("months");
        Assert.assertEquals(TimeUnit.MONTHS, timeUnitEditor.getValue());
    }
}
