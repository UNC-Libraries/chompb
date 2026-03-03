package edu.unc.lib.boxc.migration.cdm.test;

public class EadToCdmHelper {
    public static String getJsonContent(String filename) {
        var apiFilename = "no-files-included";
        if (filename != null) {
            apiFilename = filename;
        }
        return "{\"metadata\":[{\"collection_name\":\"Joyner Family Papers, ; 4428\",\"collection_number\":\"00001\"," +
                "\"location_in_collection\":\"Series 1. Correspondence, 1836-1881.\",\"citation\":\"[Identification of item], " +
                "in the Joyner Family Papers #4428, Southern Historical Collection, Wilson Special Collections Library, University " +
                "of North Carolina at Chapel Hill.\",\"filename\":\"" + apiFilename + "\",\"object_filename\":\"" + apiFilename +"\"," +
                "\"container_type\":\"Folder\",\"hook_id\":\"folder_1\",\"object\":\"Folder 1: " +
                "April 1836-15 October 1858, (17 items): Scan 1\",\"collection_url\":\"https:\\/\\/finding-aids.lib.unc.edu\\/catalog\\/04428\"," +
                "\"genre_form\":\"\",\"extent\":\"\",\"unit_date\":\"\",\"geographic_name\":\"\",\"processinfo\":\"\",\"scopecontent\":\"\"," +
                "\"unittitle\":\"April 1836-15 October 1858, (17 items)\",\"container\":\"1\"}]}";
    }
}
