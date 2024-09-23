package edu.unc.lib.boxc.migration.cdm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArchiveProjectCommandIT extends AbstractCommandIT {
    private static final String PROJECT_NAME_2 = "proj2";

    @BeforeEach
    public void setup() throws Exception {
        initProjectAndHelper();
        setupChompbConfig();
    }

    @Test
    public void archiveProjectTest() throws Exception {
        String[] args = new String[] {
                "-w", String.valueOf(baseDir),
                "archive",
                "-p", project.getProjectPath().toString()};
        executeExpectSuccess(args);

        assertTrue(Files.exists(tmpFolder.resolve("archived/" + project.getProjectName())));
    }

    @Test
    public void archiveInvalidProjectTest() throws Exception {
        String[] args = new String[] {
                "-w", String.valueOf(baseDir),
                "archive",
                "-p", tmpFolder.resolve(PROJECT_NAME_2).toString()};
        executeExpectFailure(args);

        assertOutputContains("Migration project " + tmpFolder.resolve(PROJECT_NAME_2) + " does not exist");
    }
}
