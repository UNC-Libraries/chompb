package edu.unc.lib.boxc.migration.cdm.util;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.repeat;

/**
 * Utilities for displaying progress in a CLI
 *
 * @author bbpennel
 */
public class DisplayProgressUtil {
    private static final int PROGRESS_BAR_UNITS = 40;
    private static final double PROGRESS_BAR_DIVISOR = (double) 100 / PROGRESS_BAR_UNITS;

    /**
     * Render a progress bar, percent, and total.
     *
     * For example, given current = 120 and total = 161, it would display:
     *  75% [==============================          ] 120/161
     *
     * @param current
     * @param total
     */
    public static void displayProgress(long current, long total) {
        long percent = Math.round(((float) current / total) * 100);
        int progressBars = (int) Math.round(percent / PROGRESS_BAR_DIVISOR);

        StringBuilder sb = new StringBuilder("\r");
        sb.append(format("%1$3s", percent)).append("% [");
        sb.append(repeat("=", progressBars));
        sb.append(repeat(" ", PROGRESS_BAR_UNITS - progressBars));
        sb.append("] ").append(current).append("/").append(total);
        // Append spaces to clear rest of line
        sb.append(repeat(" ", 40));
        sb.append("\r");

        System.out.print(sb.toString());
        System.out.flush();
    }

    public static void finishProgress() {
        System.out.println();
        System.out.flush();
    }

    private DisplayProgressUtil() {
    }
}