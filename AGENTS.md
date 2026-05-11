# AGENTS.md — Chompb (CDM to Box-c Migration Utility)

## Project Overview
Java 21 CLI tool (picocli) that migrates collections from CONTENTdm (CDM) to the Box-c repository. Built as a fat JAR with Maven Shade. Depends on the `box-c` parent project (must be built locally first).

## Build & Test
```bash
mvn clean package          # full build with tests
mvn test                   # run unit tests only
mvn verify                 # run all tests including integration
```
- Some ITs require external ActiveMQ (use Docker Compose from box-c project).
- Parent POM is `edu.unc.lib.cdr:cdr` (box-c repo). Must be installed locally via `mvn clean install -DskipTests` in box-c before building chompb.
- Artifact: `target/cdm2bxc.jar`; main class: `edu.unc.lib.boxc.migration.cdm.CLIMain`

## Architecture & Data Flow
```
CDM server → export XMLs → cdm_index.db (SQLite) → CSV mappings → SIPs → Box-c deposit
```
1. **Init** – creates project directory with `.project.json`
2. **Export** – fetches CDM XML records via `CdmExportService`/`CdmFileRetrievalService` (SSH or HTTP)
3. **Index** – `CdmIndexService` parses XML into SQLite (`cdm_index.db`) with entry-type metadata
4. **Map** – separate services produce CSV files: `destinations.csv`, `source_files.csv`, `access_files.csv`, `group_mappings.csv`
5. **SIP generation** – `SipService` reads all CSVs + expanded MODS descriptions, emits RDF/N3 model (`model.n3`) + deposit layout under `sips/<uuid>/`
6. **Submit** – `SipSubmissionService` sends SIPs to Box-c deposit ActiveMQ endpoint

## Key Source Directories
| Path | Purpose |
|------|---------|
| `src/main/java/.../migration/cdm/` | All commands, services, model, validators |
| `src/main/java/.../migration/cdm/services/sips/` | SIP construction (`WorkGenerator`, `WorkGeneratorFactory`, `CdmToDestMapper`) |
| `src/main/java/.../migration/cdm/model/MigrationProject.java` | Central file-path constants for every project artifact |
| `src/test/java/.../migration/cdm/test/SipServiceHelper.java` | Test harness — wires all services; reference for service dependency graph |
| `src/test/java/.../migration/cdm/AbstractCommandIT.java` | Base class for all command integration tests |

## CDM Object Types (in SQLite `entry_type` column)
Defined in `CdmIndexService`:
- `cpd_object` — compound object parent
- `cpd_child` — compound object child
- `grouped_work` — manually grouped work
- `doc_pdf` — document/PDF type

## CSV Mapping Conventions
All mapping CSVs use Apache Commons CSV. Headers are defined as constants in the corresponding `*Info` model class (e.g., `SourceFilesInfo`, `DestinationsInfo`). The first column is always the CDM object ID. A blank/missing value means unmapped.

## Configuration
`ChompbConfigService` reads a JSON config file (path passed via CLI `-c`) with `cdmEnvironments` and `bxcEnvironments` maps. See `CdmEnvironment` and `BxcEnvironment` for fields. Tests use `CdmEnvironmentHelper` / `BxcEnvironmentHelper` stubs.

## Testing Patterns
- Integration tests (`*IT.java`) extend `AbstractCommandIT` → `AbstractOutputTest`, which sets up a temp directory and a real `CommandLine(new CLIMain())` instance.
- Use `SipServiceHelper` to bootstrap a full in-memory project state (index, descriptions, mappings) before asserting SIP contents.
- Test descriptions live under `src/test/resources/descriptions/<collection>/`; CDM export fixtures under `src/test/resources/descriptions/` and `src/test/resources/files/`.
- WireMock (`wiremock`) stubs CDM HTTP API calls in export tests.

## Adding a New Command
1. Create `FooCommand.java` (picocli `@Command`) in the commands package; inject services via setters.
2. Register it in `CLIMain.java` `@Command(subcommands = {...})`.
3. Create corresponding `FooService.java` in `services/`; accept a `MigrationProject` and call `getIndexPath()` / other path accessors.
4. Add an IT extending `AbstractCommandIT`.

## Checkstyle
Checkstyle is enforced at build time: `checkstyle/checkstyle.xml` + `checkstyle/checkstyle-suppressions.xml` (inherited from box-c parent). Violations fail the build.

