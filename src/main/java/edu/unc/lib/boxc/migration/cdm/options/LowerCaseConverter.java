package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.ITypeConverter;

/**
 * Convert file extensions to lowercase (for case insensitve check of extension)
 * @author krwong
 */
public class LowerCaseConverter implements ITypeConverter<String> {
    @Override
    public String convert(String value) throws Exception {
        return value.toLowerCase();
    }
}