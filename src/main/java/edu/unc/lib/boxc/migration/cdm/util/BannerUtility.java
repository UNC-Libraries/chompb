/**
 * Copyright 2008 The University of North Carolina at Chapel Hill
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
