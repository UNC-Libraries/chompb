package edu.unc.lib.boxc.migration.cdm.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

/**
 * @author bbpennel
 */
public class BannerUtility {
    private static final Logger log = getLogger(BannerUtility.class);

    private BannerUtility() {
    }

    public static String getBanner() {
        return loadFile("banner.txt");
    }

    private static String loadFile(String rescPath) {
        InputStream stream = BannerUtility.class.getResourceAsStream("/" + rescPath);
        try {
            return IOUtils.toString(stream, UTF_8);
        } catch (IOException e) {
            log.error("Failed to load banner", e);
            return "";
        }
    }
}
