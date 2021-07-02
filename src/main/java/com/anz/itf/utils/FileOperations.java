package com.anz.itf.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileOperations {

    /**
     * This method reads the tag file and returns it content in a HashMap
     * @param fileName
     * @return
     */
    public HashMap<String,String> readTagFile(String fileName) {
        HashMap<String,String> tagFileContentsMap = new HashMap<>();
        try {
            File myObj = new File(fileName);
            Scanner myReader = new Scanner(myObj);
            //while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            String[] value_split = data.split("\\|");
            tagFileContentsMap.put("fileName",value_split[0]);
            tagFileContentsMap.put("recordCount",value_split[1]);
            //}
            myReader.close();
        } catch (FileNotFoundException e){
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return tagFileContentsMap;
    }

    /**
     * Compares two files in distributed manner
     * @param fileName1
     * @param fileName2
     * @return
     * @throws Exception
     */
    public boolean compareTwoFiles(String fileName1, String fileName2) throws Exception {
        File file1 = new File(fileName1);
        File file2 = new File(fileName2);

        boolean isTwoEqual = FileUtils.contentEquals(file1, file2);
        return isTwoEqual;
    }

    /**
     * This method moves the part* files from directory (eg. aus-capital.csv) to one level up i.e. output folder
     * and also deletes the _SUCCESS file from it. It renames dir to dir_Tmp first.
     * @param spark
     * @param newDfWithDirtyFlag
     * @param inputArgsMap
     * @return
     * @throws Exception
     */
    public void moveWrittenFileToTopLevel(SparkSession spark, Dataset<Row> newDfWithDirtyFlag, HashMap<String, String> inputArgsMap) throws Exception {

        String output = inputArgsMap.get("actualOutput");
        File sourceDir = new File(output);
        File destDir = new File(output + "Tmp");

        if (sourceDir.renameTo(destDir)) {
            System.out.println("Dir renamed successfully");
        } else {
            System.out.println("failed to rename dir");
        }

        Path path = Paths.get(output);
        Path fileNameWithoutFolderFromInputCommand = path.getFileName();
        Path parentName = path.getParent();

        File dir = new File(output + "Tmp");
        System.out.println(dir.listFiles());

        File[] files = dir.listFiles((d, name) -> name.startsWith("part"));
        if (files!=null && files.length != 0) {
            for (int i = 0; i < files.length; i++) {
                File sourceFile = new File(output + "Tmp" + "/" + files[i].getName());
                File destinationFile = new File(parentName.toString() + "/" + fileNameWithoutFolderFromInputCommand.toString());
                System.out.println("sourceFile is " + sourceFile.getName());
                System.out.println("destinationFile is " + destinationFile.getName());

                if (sourceFile.renameTo(destinationFile)) {
                    System.out.println("Directory renamed successfully");
                } else {
                    System.out.println("Failed to rename directory");
                }
            }

            File file = new File(output + "Tmp" + "/_SUCCESS");
            if (file.delete()) {
                System.out.println(file.getName() + " is deleted!");
            } else {
                System.out.println("Sorry, unable to delete the file.");
            }
            FileUtils.deleteDirectory(new File(output + "Tmp"));
        }
    }
}


