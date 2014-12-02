package com.petpal.tracking.integration;

import java.util.Arrays;
import java.util.List;

/**
 * Created by per on 12/1/14.
 */
public enum TestAggregationLevel {
    HOURS,
    DAYS,
    WEEKS,
    MONTHS,
    YEARS;

    public static List<TestAggregationLevel> getAllTestAggregationLevels() {
        return Arrays.asList(values());
    }
}
