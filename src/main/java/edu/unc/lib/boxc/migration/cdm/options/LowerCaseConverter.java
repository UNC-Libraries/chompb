package edu.unc.lib.boxc.migration.cdm.options;

import picocli.CommandLine.ITypeConverter;

public class LowerCaseConverter implements ITypeConverter<String> {
    @Override
    public String convert(String value) throws Exception {
        return value.toLowerCase();
    }
}