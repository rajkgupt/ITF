package com.anz.itf.validation;

import com.anz.itf.utils.FileOperations;
import com.anz.itf.utils.ParseOptions;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.hadoop.fs.*;

import static org.apache.spark.sql.functions.col;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class RunValidationHelper {
    Long totalRecordCountFromDF = null;
    Long recordCountFromTagFile = null;
    String dataFileNameFromTagFile = null;
    String fileNameWithFolderFromInputCommand = null;
    long missingOrAdditionalColumnCount = 0;


    public Dataset<Row> getDataFrame(SparkSession spark, HashMap<String,String> inputArgsMap) {

        //TODO - handle date format error
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        //TODO - handle schema in generic manner - from input argument json file?
        StructType schema = new StructType(new StructField[]{
                new StructField("State/Territory"     , DataTypes.StringType ,false, Metadata.empty()),
                new StructField("Capital"   ,DataTypes.StringType  ,false, Metadata.empty()),
                new StructField("City Population"     ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("State/Territory Population"   ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("Percentage"   ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("Established"   ,DataTypes.DateType  ,false, Metadata.empty()),
                new StructField("_corrupt_record"   ,DataTypes.StringType  ,false, Metadata.empty())
        });

        Dataset<Row> inputFileDF  = spark.read().format("csv")
                .option("header", "true")
                .option("dateFormat", "dd-MM-yyyy")
                .schema(schema)
                .load(inputArgsMap.get("data")).cache();

        return inputFileDF;
    }

    public static void main(String[] args)  {
        Options options = ParseOptions.OPTIONS;
    }

    /**
     * Performs record Count checks
     * @param inputFileDF
     * @param inputArgsMap
     * @return 0 if records Count match other returns 1
     */
    public int getRecordCountCheck(Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        //Prep to perform FileName & Record count checks
        // Get FileName and recordCount from tag file
        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));


        //Operation for recordCount check
        totalRecordCountFromDF = inputFileDF.count();
        recordCountFromTagFile = Long.parseLong(tagFileContents.get("recordCount"));
        System.out.println("totalRecordCountFromDF calculated is:" + totalRecordCountFromDF);
        System.out.println("recordCountFromTagFile found is:" + recordCountFromTagFile);


        int recordCount = 0;

        if (totalRecordCountFromDF != recordCountFromTagFile)
            return 1 ;
        else
            return 0;
    }

    /**
     * checks the fileName
     * @param inputFileDF
     * @param inputArgsMap
     * @return 0 if matches else returns 2
     */
    public int getFileNameCheck(Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));

        //Operation for fileName check
        fileNameWithFolderFromInputCommand = inputArgsMap.get("data");

        // create object of Path
        Path path = Paths.get(fileNameWithFolderFromInputCommand);
        // call getFileName() and get FileName path object
        Path fileNameWithoutFolderFromInputCommand = path.getFileName();

        dataFileNameFromTagFile = tagFileContents.get("fileName");
        System.out.println("fileNameWithoutFolderFromInputCommand calculated is:" + fileNameWithoutFolderFromInputCommand);
        System.out.println("dataFileNameFromTagFile found is:" + dataFileNameFromTagFile);


        if (fileNameWithoutFolderFromInputCommand.toString().equals(dataFileNameFromTagFile))
            return 0;
        else
            return 2;
    }

    /**
     *
     * @param spark
     * @param inputFileDF
     * @param inputArgsMap
     * @return 0 if no violation, else return 3
     */
    public int getPrimaryKeyCount(SparkSession spark,
                                                    Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {


        //Prep to perform Primary key & Missing or additional columns check
        //Register the dataframe with table
        //inputFileDF.createOrReplaceTempView("ausInputFileTable");

        long countBeforeDroppingDupes = inputFileDF.count();

        String primaryKeysArray[] = { "State/Territory"};

        //Operation for primary key violation check
        Dataset<Row> primaryKeyCountDF = inputFileDF.dropDuplicates(primaryKeysArray);
        long countAfterDroppingDupes = primaryKeyCountDF.count();

        System.out.println("countBeforeDroppingDupes calculated is:" + countBeforeDroppingDupes);
        System.out.println("countAfterDroppingDupes calculated is:" + countAfterDroppingDupes);

        if (countBeforeDroppingDupes == countAfterDroppingDupes)
            return 0;
        else
            return 3;


    }

    /**
     *
     * @param spark
     * @param inputFileDF
     * @param inputArgsMap
     * @return 0 if no violation else 4
     */
    public int getMissingOrAdditionalColumnCount(SparkSession spark,
                                                                   Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        inputFileDF.createOrReplaceTempView("ausInputFileTable");
        //Operations to check for missing or additional columns
        Dataset<Row> missingOrAdditionalColumnCountDF =
                spark.sql("select count(*) as count1 from ausInputFileTable where _corrupt_record is not null");
        Row missingOrAdditionalColumnCountRow = missingOrAdditionalColumnCountDF.select(col( "count1")).first();
        missingOrAdditionalColumnCount = missingOrAdditionalColumnCountRow.getAs("count1");
        System.out.println("missingOrAdditionalColumnCount calculated is:" + missingOrAdditionalColumnCount);

        HashMap<String, Long> missingOrAdditionalColumnCountMap = new HashMap<>();
        missingOrAdditionalColumnCountMap.put("missingOrAdditionalColumnCount",missingOrAdditionalColumnCount);
        inputFileDF.show();

        if (0 == missingOrAdditionalColumnCount)
            return 0;
        else
            return 4;
    }

    public Dataset<Row> getDataFrameWithDirtyFlag(SparkSession spark, Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        Dataset<Row>newDf = inputFileDF.withColumn("dirty_flag", functions.when(functions.col("_corrupt_record").isNotNull(), 1)
                .otherwise(0));
        newDf = newDf.drop("_corrupt_record");
        newDf.show();

        return newDf;
    }


    public String writeDataFrame(SparkSession spark, Dataset<Row> newDfWithDirtyFlag, HashMap<String, String> inputArgsMap) throws Exception{
        newDfWithDirtyFlag.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(inputArgsMap.get("actualOutput"));



        String output = inputArgsMap.get("actualOutput");
        Path path = Paths.get(output);
        // call getFileName() and get FileName path object
        Path fileNameWithoutFolderFromInputCommand = path.getFileName();
        Path parentName = path.getParent();

        File dir = new File(output+"Tmp");
        System.out.println(dir.listFiles());

        File[] files = dir.listFiles((d, name) -> name.startsWith("part"));

        for (int i = 0; i < files.length; i++) {
            File sourceFile = new File (output + "Tmp" + "/" + files[i].getName());
            File destinationFile = new File(parentName.toString() + "/" + fileNameWithoutFolderFromInputCommand.toString());
            System.out.println("sourceFile is " + sourceFile.getName());
            System.out.println("destinationFile is " + destinationFile.getName());

            if (sourceFile.renameTo(destinationFile)) {
                System.out.println("Directory renamed successfully");
            } else {
                System.out.println("Failed to rename directory");
            }
        }

        File file = new File (output + "Tmp" + "/_SUCCESS");;
        if (file.delete()) {
            System.out.println(file.getName() + " is deleted!");
        } else {
            System.out.println("Sorry, unable to delete the file.");
        }

        Path path1 = Paths.get(output + "Tmp");
        java.nio.file.Files.delete(path1);




        //df.write().mode(SaveMode.Overwrite).csv("newcars.csv");

        return "Success";

    }
}
