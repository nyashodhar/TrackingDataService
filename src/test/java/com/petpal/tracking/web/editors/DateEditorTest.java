package com.petpal.tracking.web.editors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

/**
 * Created by per on 10/30/14.
 */
public class DateEditorTest {

    // Class under test
    private DateEditor dateEditor;

    @Before
    public void setup() {
        dateEditor = new DateEditor();
    }

    @Test
    public void test_setAsText() {
        long timestamp = 1401408207653L;
        dateEditor.setAsText(Long.toString(timestamp));
        Assert.assertEquals(timestamp, ((Date)dateEditor.getValue()).getTime());
    }

}
