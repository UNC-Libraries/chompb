package edu.unc.lib.boxc.migration.cdm.services;

import edu.unc.lib.boxc.migration.cdm.exceptions.InvalidProjectStateException;
import edu.unc.lib.boxc.migration.cdm.exceptions.MigrationException;
import edu.unc.lib.boxc.migration.cdm.model.CdmFieldInfo;
import edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo;
import edu.unc.lib.boxc.migration.cdm.options.CdmIndexOptions;
import edu.unc.lib.boxc.migration.cdm.util.ProjectPropertiesSerialization;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.ID_FIELD;
import static edu.unc.lib.boxc.migration.cdm.model.SourceFilesInfo.SOURCE_FILE_FIELD;
import static edu.unc.lib.boxc.migration.cdm.services.CdmFieldService.EAD_TO_CDM;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.CITATION;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.COLLECTION_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.COLLECTION_NUMBER;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.COLLECTION_URL;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.CONTAINER;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.CONTAINER_TYPE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.EXTENT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.FILENAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.GENRE_FORM;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.GEOGRAPHIC_NAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.HOOK_ID;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.LOC_IN_COLLECTION;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.OBJECT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.OBJ_FILENAME;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.PROCESS_INFO;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.REF_ID;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.SCOPE_CONTENT;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.TSV_HEADERS;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.TSV_WITH_ID_HEADERS;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.UNIT_DATE;
import static edu.unc.lib.boxc.migration.cdm.util.EadToCdmUtil.UNIT_TITLE;

/**
 * Service for populating the index of a CDM project via a file (CSV or EAD to CDM TSV)
 * @author snluong
 */
public class FileIndexService extends IndexService {
    private String source;

    /**
     * Indexes all exported objects for this project
     * @param options
     * @throws IOException
     */
    public void indexAllFromFile(CdmIndexOptions options) throws IOException {
        var path = getPath(options);
        assertFileImportExists(path);

        CdmFieldInfo fieldInfo = fieldService.loadFieldsFromProject(project);
        var exportFields = fieldInfo.listAllExportFields();
        exportFields.addAll(MIGRATION_FIELDS);
        setRecordInsertSqlTemplate(makeInsertTemplate(exportFields));

        var format = CSVFormat.DEFAULT;
        var header = exportFields.toArray(new String[0]);
        var readerPath = path;
        if (Objects.equals(source, EAD_TO_CDM)) {
            var eadToCdmWithIdPath = addIdsToEadToCdmTsv(path);
            header = TSV_WITH_ID_HEADERS;
            format = CSVFormat.TDF;
            readerPath = eadToCdmWithIdPath;
        }

        var csvFormat = format.builder()
                .setTrim(true)
                .setSkipHeaderRecord(true)
                .setHeader(header)
                .get();

        try (
                var conn = openDbConnection();
                var reader = Files.newBufferedReader(readerPath);
                var csvParser = CSVParser.parse(reader, csvFormat);
        ) {
            for (CSVRecord csvRecord : csvParser) {
                if (!csvRecord.get(0).isEmpty()) {
                    List<String> fieldValues = csvRecord.toList();
                    indexObject(conn, fieldValues);
                }
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to read export files", e);
        } catch (SQLException e) {
            throw new MigrationException("Failed to update database", e);
        } catch (IllegalArgumentException e) {
            throw new MigrationException("Invalid arguments", e);
        }

        project.getProjectProperties().setIndexedDate(Instant.now());
        ProjectPropertiesSerialization.write(project);
    }

    private void assertFileImportExists(Path path) {
        if (Files.notExists(path)) {
            throw new InvalidProjectStateException("User provided csv/tsv must exist prior to indexing");
        }
    }

    public Path addIdsToEadToCdmTsv(Path eadToCdmTsvPath) {
        var eadToCdmWithIdPath = project.getProjectPath().resolve("ead_to_cdm_with_ids.tsv");
        var filenameToIdMap = getIdsFromSourceFile();
        var format = CSVFormat.TDF.builder()
                .setTrim(true)
                .setSkipHeaderRecord(true)
                .setHeader(TSV_HEADERS)
                .get();

        var printerFormat = CSVFormat.TDF.builder()
                .setTrim(true)
                .setHeader(TSV_WITH_ID_HEADERS)
                .get();
        try (
                var reader = Files.newBufferedReader(eadToCdmTsvPath);
                var tsvParser = CSVParser.parse(reader, format);

                var writer = Files.newBufferedWriter(eadToCdmWithIdPath);
                CSVPrinter tsvPrinter = new CSVPrinter(writer, printerFormat);
        ) {
            for (CSVRecord tsvRecord : tsvParser) {
                var filename = tsvRecord.get(FILENAME);
                var cdmId = filenameToIdMap.get(filename);
                if (cdmId == null) {
                    throw new IllegalArgumentException("No CDM ID found for EAD to CDM record for filename: "
                            + filename);
                }
                tsvPrinter.printRecord(tsvRecord.get(COLLECTION_NAME), tsvRecord.get(COLLECTION_NUMBER),
                        tsvRecord.get(LOC_IN_COLLECTION), tsvRecord.get(CITATION), filename,
                        tsvRecord.get(OBJ_FILENAME), tsvRecord.get(CONTAINER_TYPE), tsvRecord.get(HOOK_ID),
                        tsvRecord.get(OBJECT), tsvRecord.get(COLLECTION_URL), tsvRecord.get(GENRE_FORM),
                        tsvRecord.get(EXTENT), tsvRecord.get(UNIT_DATE), tsvRecord.get(GEOGRAPHIC_NAME),
                        tsvRecord.get(REF_ID), tsvRecord.get(PROCESS_INFO), tsvRecord.get(SCOPE_CONTENT),
                        tsvRecord.get(UNIT_TITLE), tsvRecord.get(CONTAINER), cdmId);
            }
            return eadToCdmWithIdPath;
        } catch (IOException e) {
            throw new MigrationException("Unable to format new TSV", e);
        }
    }

    private Map<String, String> getIdsFromSourceFile() {
        Map<String, String> filenameToId = new HashMap<>();
        var sourceFilesPath = project.getSourceFilesMappingPath();
        var format = CSVFormat.DEFAULT.builder()
                .setTrim(true)
                .setSkipHeaderRecord(true)
                .setHeader(SourceFilesInfo.CSV_HEADERS)
                .get();
        try (
                var reader = Files.newBufferedReader(sourceFilesPath);
                var csvRecords = CSVParser.parse(reader, format);
        ) {
            for (var record : csvRecords) {
                var basePath = Paths.get(record.get(SOURCE_FILE_FIELD));
                var filename = basePath.getFileName().toString();
                filenameToId.put(filename, record.get(ID_FIELD));
            }
        } catch (IOException e) {
            throw new MigrationException("Failed to get source files info", e);
        }

        return filenameToId;
    }

    private Path getPath(CdmIndexOptions options) {
        if (Objects.equals(source, EAD_TO_CDM)) {
            return options.getEadTsvFile();
        }
        return options.getCsvFile();
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
