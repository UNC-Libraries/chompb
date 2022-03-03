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
package edu.unc.lib.boxc.migration.cdm.services;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.SheetConditionalFormatting;
import org.apache.poi.ss.usermodel.ConditionalFormattingRule;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.PatternFormatting;
import org.apache.poi.ss.util.CellRangeAddress;

import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.MigrationProject;

/**
 * Service for generating field assessment spreadsheet template
 * @author krwong
 */
public class FieldAssessmentTemplateService {
    private CdmFieldService cdmFieldService;
    private static final List<String> COLUMN_NAMES = Arrays.asList("Field Label", "Alias", "MD Source",
            "All fields blank?", "Some fields blank?", "Repeated fields?", "Retain?", "Display?",
            "MODS mapping", "Notes", "Remediation needed", "cdm_required", "cdm_searchable", "cdm_hidden",
            "cdm_vocab", "cdm_dc_mapping");

    /**
     * Generate field assessment spreadsheet template for a collection
     * @param project
     * @throws IOException
     */
    public void generate(MigrationProject project) throws IOException {
        // Get a list of all the fields
        cdmFieldService.validateFieldsFile(project);
        CdmFieldInfo fieldInfo = cdmFieldService.loadFieldsFromProject(project);
        List<CdmFieldInfo.CdmFieldEntry> fields = fieldInfo.getFields().stream()
                .filter(f -> !f.getSkipExport())
                .collect(Collectors.toList());

        // Create workbook and sheet
        XSSFWorkbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Template");

        // Style spreadsheet
        XSSFFont defaultFont = workbook.createFont();
        defaultFont.setFontHeightInPoints((short) 11);
        defaultFont.setFontName("Calibri");
        defaultFont.setColor(IndexedColors.BLACK.getIndex());
        defaultFont.setBold(false);
        defaultFont.setItalic(false);

        XSSFFont fontBold = workbook.createFont();
        fontBold.setFontHeightInPoints((short) 11);
        fontBold.setFontName("Calibri");
        fontBold.setColor(IndexedColors.WHITE.getIndex());
        fontBold.setBold(true);
        fontBold.setItalic(false);

        XSSFCellStyle defaultStyle = workbook.createCellStyle();
        defaultStyle.setFont(defaultFont);

        XSSFCellStyle headerStyle = workbook.createCellStyle();
        headerStyle.setFillForegroundColor(new XSSFColor(new java.awt.Color(0, 102, 204), null));
        headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        headerStyle.setFont(fontBold);

        SheetConditionalFormatting sheetCF = sheet.getSheetConditionalFormatting();
        ConditionalFormattingRule rule1 = sheetCF.createConditionalFormattingRule("=MOD(ROW(),2)=0");
        PatternFormatting fill = rule1.createPatternFormatting();
        XSSFColor color = new XSSFColor(new java.awt.Color(0, 102, 204), null);
        color.setTint(0.85);
        fill.setFillBackgroundColor(color);
        fill.setFillPattern(PatternFormatting.SOLID_FOREGROUND);
        CellRangeAddress[] regions = {CellRangeAddress.valueOf("A1:Z200")};
        sheetCF.addConditionalFormatting(regions, rule1);

        // Add a header row, and freeze it
        Row header = sheet.createRow(0);
        sheet.setAutoFilter(new CellRangeAddress(0, 0, 0, 15));
        sheet.createFreezePane(0,1);
        header.setRowStyle(headerStyle);
        int cellNum = 0;
        for (String name : COLUMN_NAMES) {
            header.createCell(cellNum).setCellValue(name);
            cellNum++;
        }

        // Add a row for every field in field info object and fill in default values for all blank ? fields
        for (int i = 0; i < fields.size(); i++) {
            Row row = sheet.createRow(i + 1);
            row.setRowStyle(defaultStyle);
            row.createCell(0).setCellValue(fields.get(i).getDescription());
            row.createCell(1).setCellValue(fields.get(i).getExportAs());
            row.createCell(3).setCellValue("n");
            row.createCell(4).setCellValue("n");
            row.createCell(5).setCellValue("n");
            row.createCell(6).setCellValue("n");
            row.createCell(7).setCellValue("n");
            row.createCell(11).setCellValue(fields.get(i).getCdmRequired());
            row.createCell(12).setCellValue(fields.get(i).getCdmSearchable());
            row.createCell(13).setCellValue(fields.get(i).getCdmHidden());
            row.createCell(14).setCellValue(fields.get(i).getCdmVocab());
            row.createCell(15).setCellValue(fields.get(i).getCdmDcMapping());
        }

        // Auto size the column widths
        for (int columnIndex = 0; columnIndex < COLUMN_NAMES.size(); columnIndex++) {
            sheet.autoSizeColumn(columnIndex);
            int currentColumnWidth = sheet.getColumnWidth(columnIndex);
            sheet.setColumnWidth(columnIndex, (currentColumnWidth + 1000));
        }

        Path projPath = project.getProjectPath();
        String filename = "field_assessment_" + project.getProjectProperties().getName() + ".xlsx";
        OutputStream outputStream = Files.newOutputStream(projPath.resolve(filename));
        workbook.write(outputStream);
        workbook.close();
    }

    public void setCdmFieldService(CdmFieldService cdmFieldService) {
        this.cdmFieldService = cdmFieldService;
    }
}
