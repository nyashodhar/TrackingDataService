package com.petpal.tracking.web.controllers;

import java.util.Arrays;
import java.util.List;

/**
 * Created by per on 12/1/14.
 */
public enum AggregationLevel {
    HOURS,
    DAYS,
    WEEKS,
    MONTHS,
    YEARS;

    public static List<AggregationLevel> getAllAggregationLevels() {
        return Arrays.asList(values());
    }
}