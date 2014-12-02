package com.petpal.tracking.web.editors;

import com.petpal.tracking.web.controllers.AggregationLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by per on 11/24/14.
 */
public class AggregationLevelEditorTest {

    // Class under test
    private AggregationLevelEditor aggregationLevelEditor;

    @Before
    public void setup() {
        aggregationLevelEditor = new AggregationLevelEditor();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setAsText_null() {
        aggregationLevelEditor.setAsText(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_setAsText_invalid() {
        aggregationLevelEditor.setAsText("something");
    }

    @Test
    public void test_setAsText_valid() {
        aggregationLevelEditor.setAsText("YearS");
        Assert.assertEquals(AggregationLevel.YEARS, aggregationLevelEditor.getValue());

        aggregationLevelEditor.setAsText("months");
        Assert.assertEquals(AggregationLevel.MONTHS, aggregationLevelEditor.getValue());
    }
}
