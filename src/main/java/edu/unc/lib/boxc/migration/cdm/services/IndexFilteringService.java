package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.options.IndexFilteringOptions;

import java.util.Map;

/**
 * Service for filtering the cdm index to a subset of the original items
 * @author bbpennel
 */
public class IndexFilteringService {
    /**
     * Calculates the number of items that would be left in the index if the provided filter were applied
     * @param options filtering options
     * @return A map with two values, "remainder" => count of items that would remain,
     *      and "total" => count of items before filtering.
     */
    public Map<String, Integer> calculateRemainder(IndexFilteringOptions options) {


        return Map.of("remainder", 0,
                "total", 0);
    }

    /**
     * Filter the index to a subset of the original items based on the provided filtering options
     * @param options filtering options
     */
    public void filterIndex(IndexFilteringOptions options) {

    }
}
