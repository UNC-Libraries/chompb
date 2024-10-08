# Chomping Block (chompb)
A commandline utility which facilitates the migration of collections from a CONTENTdm server to a Box-c repository.

# Building and developing

## Requirements
Requires Java 8 in order to run.
Building the project requires maven 3.
The project has only been tested in Mac and Linux environments

## Initial project setup
When first setting up the project for development purposes, you will need to perform the following steps:

* First, make a local clone of the box-c repo and build it
>[Box-C Build Steps](https://github.com/UNC-Libraries/box-c#requirements)

* Next, clone the migration utility and build it
```
# in your home directory
git clone git@github.com:UNC-Libraries/cdm-to-boxc-migration-util.git
cd cdm-to-boxc-migration-util
mvn clean install
```

### Building and updating
In order to perform a full build of the project (with tests run) you would perform:
```
mvn clean package
```

If there are updates to the box-c project which need to be pulled in for use in the migration utility, you will need to do a `pull` in the box-c clone (or make the changes locally), and build the box-c project with a `mvn clean install -DskipTests`.

### Running tests
The tests can be run in intellij or from the command line with `mvn test` or `mvn verify`.

A few of the tests depend on Redis to be running externally. The preferred way to run Redis is to use the docker-compose file in the box-c project, see the readme in box-c for instructions. After building it the first time, you can start the Redis container directly in Docker Desktop.

### Deploying
In order to deploy the project to a server or the development VM, see the `deploy_migration_util.rb` command from the `boxc-ansible` project. You can deploy uncommitted changes to the utility by providing the `-p` option. For example, `./deploy_migration_util.rb dev -p /path/to/cdm-to-boxc-migration-util` would build and deploy the current state of the migration util located at the provided path.

# Usage

## Basic Usage on Servers
```
chompb -h
```

## General Workflow
1. Initialize a new migration project for a CDM collection
2. Export object records from CDM
3. Index the exported object records
4. Add data to the migration project, in any order:
	1. Map objects to Box-c destinations
	2. Add MODS descriptions for objects and new collections
	3. Map objects to source files
	4. Map objects to access files (optional)
5. Once all of these steps are complete and the migration team signs off...
6. Perform transformation of migration project to one or more SIPs for deposit (number of SIPs is based on the destination mappings)
7. Submit SIPs to Box-c deposit service for ingest

### Example workflow for Gilmer
```
# Initialize a new migration project (named gilmer_demo, from CDM collection gilmer)
chompb init -p gilmer_demo -c gilmer

cd gilmer_demo
# Export object records from CDM
chompb export -p
# you will be prompted for your password

# Index the exported object records
chompb index

# Map objects to Box-c destination 
# to map all objects being migrated by default into a newly created "00276" collection in existing Box-c Admin Unit with id 4e282ae9-496d-48bc-b1cc-a59ee565efa8
chompb destinations generate -dd 4e282ae9-496d-48bc-b1cc-a59ee565efa8 -dc 00276

# Map objects to source files
chompb source_files generate -b /path/to/00276_gilmer_maps/ -p "([0-9]+)\\_([^_]+)\\_E.tiff?" -t '00$1_op0$2_0001.tif' -l
chompb source_files generate -b /path/to/00276_gilmer_maps/enhanced/ -p "([0-9]+)\\_([^_]+)\\_E.tiff?" -t '00$1_op0$2_0001_e.tif' -l -u 
# Optional: Map objects to access files
chompb access_files generate -b /path/to/00276_gilmer_maps/enhanced/ -p "([0-9]+)\\_([^_]+)\\_E.tiff?" -t '00$1_op0$2_0001_e.tif' -l -d

# Creation of MODS descriptions is not performed by the migration utility
# Descriptions for objects being migrated should be placed in the "descriptions" folder, encoded as modsCollections
# Descriptions for new collections should be placed in the "newCollectionDescriptions", encoded as mods records
# For demo purposes, dummy descriptions can be generated for all objects with the following command:
# chompb descriptions generate

# Perform transformation of migration project to one or more SIPs for deposit
chompb descriptions expand
chompb sips generate

# Submit all SIPs for deposit (optionally, individual SIPs can be submitted)
chompb submit
# NOTE: in order to submit the SIPs, they must be located in an approved staging location.
# All prior steps can be perform at any path on the server.

# Index redirect mapping data from the CSV to the database (based on environment)
chompb index_redirects
```

### Monitoring progress and project state
In order to view the overall status of a migration project you may use the status command:
```
chompb status
```
Additionally, there are a number of more detailed status reports available for individual components of the migration, such as:
```
chompb descriptions status
chompb source_files status
chompb access_files status
chompb destinations status
```

There are also commands available to validate various aspects of the project:
```
chompb descriptions validate # validates MODS descriptions against schemas and local schematron
chompb source_files validate # verifies syntax, whether mapped files exist, and other concerns
chompb access_files validate
chompb destinations validate # verifies syntax and consistency of mappings
```

# Project Directory Structure
A migration project initialized using this utility will produce a directory with the following structure:
* <project name> - root directory for the project, named using the project name provided during initialization
	* .project.json - Properties tracked by the migration utility about the current migration project.
	* access_files.csv - Produced by the `access_files` command. Maps CDM IDs to paths where access files to be migrated are located.
	* cdm_fields.csv - Produced by the `init` command. Contains the list of fields for the CDM collection being migrated. This file may be edited in order to rename or exclude fields prior to `export`. Nicknames and export names must be unique.
	* cdm_index.db - Produced by the `index` command. This is a sqlite3 database containing all of the exported data for the CDM collection being migrated. It can be edited, but should be handled with care as it will be used for almost all other commands.
	* descriptions/ - Directory in which user produced mods:modsCollection files should be placed, containing mods records for objects being migrated. The names of the files do not matter, except that they must be .xml files. The MODS records within the collections must contain a CDM IDs in the form `<mods:identifier type="local" displayLabel="CONTENTdm number">27</mods:identifier>`.
	* destinations.csv - Produced by the `destinations` command. CSV document which maps CDM ids to Box-c deposit destinations, and optionally, new collections.
	* exports/ - Directory generated by the `export` command. Contains one or more .xml files containing exported CDM records.
  	* field_assessment_<project>.xlsx - Generated by the `fields generate_report` command. This is a field assessment spreadsheet with data pulled from cdm_fields.csv and prepopulated "n" values (all columns ending with ?). 
	* newCollectionDescriptions/ - Directory in which user produced mods:mods documents describing newly generated Box-c collections should be placed. The files must be named using the same identifier used for the new collections in the destination.csv file.
  	* redirect_mappings.csv - Generated by the `sips` command. Contains a CSV with four columns: `cdm_collection_id`, `cdm_object_id`, `boxc_object_id`, `boxc_file_id`. Used for redirects on the server (must be manually indexed).
	* sips/ - Directory generated by the `sips` command. Contains submission information packages produced for submission to the repository. Within this directory will by subdirectories named based on the UUID of the deposit produced. The contents of these directories follows the standard box-c deposit pipeline layout.
	* source_files.csv - Produced by the `source_files` command. Maps CDM IDs to paths where source files to be migrated are located.
