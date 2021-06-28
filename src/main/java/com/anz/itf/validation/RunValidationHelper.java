package com.anz.itf.validation;

import com.anz.itf.utils.FileOperations;
import com.anz.itf.utils.ParseOptions;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.*;

import java.util.HashMap;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;


public final class RunValidationHelper {
    Long totalRecordCountFromDF = null;
    Long recordCountFromTagFile = null;
    String dataFileNameFromTagFile = null;
    String fileNameWithFolderFromInputCommand = null;
    String fileNameWithoutFolderFromInputCommand = null;
    long primaryKeyCount = 0;
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
                .schema(schema)
                .load(inputArgsMap.get("data")).cache();

        return inputFileDF;
    }

    public static void main(String[] args)  {
        Options options = ParseOptions.OPTIONS;
    }

    public HashMap<String, Long> getRecordCountCheck(Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        //Prep to perform FileName & Record count checks
        // Get FileName and recordCount from tag file
        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));


        //Operation for recordCount check
        totalRecordCountFromDF = inputFileDF.count();
        recordCountFromTagFile = Long.parseLong(tagFileContents.get("recordCount"));
        System.out.println("totalRecordCountFromDF calculated is:" + totalRecordCountFromDF);
        System.out.println("recordCountFromTagFile found is:" + recordCountFromTagFile);

        HashMap<String, Long> recordCountMap = new HashMap<>();
        recordCountMap.put("totalRecordCountFromDF",totalRecordCountFromDF);
        recordCountMap.put("recordCountFromTagFile",recordCountFromTagFile);

        return recordCountMap;
    }

    public HashMap<String, String> getFileNameCheck(Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));

        //Operation for fileName check
        fileNameWithFolderFromInputCommand = inputArgsMap.get("data");
        String[] fileFolderNamesArray = fileNameWithFolderFromInputCommand.split("/");
        fileNameWithoutFolderFromInputCommand = fileFolderNamesArray[1];
        dataFileNameFromTagFile = tagFileContents.get("fileName");
        System.out.println("fileNameWithoutFolderFromInputCommand calculated is:" + fileNameWithoutFolderFromInputCommand);
        System.out.println("dataFileNameFromTagFile found is:" + dataFileNameFromTagFile);

        HashMap<String, String> fileNameCheckMap = new HashMap<>();
        fileNameCheckMap.put("fileNameWithoutFolderFromInputCommand",fileNameWithoutFolderFromInputCommand);
        fileNameCheckMap.put("dataFileNameFromTagFile",dataFileNameFromTagFile);

        return fileNameCheckMap;
    }

    public HashMap<String,Long>  getPrimaryKeyCount(SparkSession spark,
                                                    Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {


        //Prep to perform Primary key & Missing or additional columns check
        //Register the dataframe with table
        inputFileDF.createOrReplaceTempView("ausInputFileTable");

        //Operation for primary key violation check
        Dataset<Row> primaryKeyCountDF = spark.sql("WITH " +
                "t AS (select `State/Territory`,count(*) as count1 from ausInputFileTable group by `State/Territory` having count(*) > 1)," +
                " t2 AS ( SELECT count(*) as count2 FROM t where count1 <> 1 )" +
                "SELECT count2 FROM t2");
        Row primaryKeyCountRow = primaryKeyCountDF.select(col( "count2")).first();
        primaryKeyCount = primaryKeyCountRow.getAs("count2");
        System.out.println("primaryKeyCount calculated is:" + primaryKeyCount);


        HashMap<String, Long> primaryKeyCountMap = new HashMap<>();
        primaryKeyCountMap.put("primaryKeyCount",primaryKeyCount);
        inputFileDF.show();

        return primaryKeyCountMap;

    }

    public HashMap<String, Long> getMissingOrAdditionalColumnCount(SparkSession spark,
                                                                   Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {


        //Operations to check for missing or additional columns
        Dataset<Row> missingOrAdditionalColumnCountDF =
                spark.sql("select count(*) as count1 from ausInputFileTable where _corrupt_record is not null");
        Row missingOrAdditionalColumnCountRow = missingOrAdditionalColumnCountDF.select(col( "count1")).first();
        missingOrAdditionalColumnCount = missingOrAdditionalColumnCountRow.getAs("count1");
        System.out.println("missingOrAdditionalColumnCount calculated is:" + missingOrAdditionalColumnCount);

        HashMap<String, Long> missingOrAdditionalColumnCountMap = new HashMap<>();
        missingOrAdditionalColumnCountMap.put("missingOrAdditionalColumnCount",missingOrAdditionalColumnCount);
        inputFileDF.show();
        return missingOrAdditionalColumnCountMap;
    }

    public Dataset<Row> getDataFrameWithDirtyFlag(SparkSession spark, Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        Dataset<Row>newDf = inputFileDF.withColumn("dirty_flag", functions.when(functions.col("_corrupt_record").isNotNull(), 1)
                .otherwise(0));
        newDf = newDf.drop("_corrupt_record");
        newDf.show();

        return newDf;
    }


    public String writeDataFrame(SparkSession spark, Dataset<Row> newDfWithDirtyFlag, HashMap<String, String> inputArgsMap) {
        newDfWithDirtyFlag.write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", "true")
                .save(inputArgsMap.get("ExpectedOutput"));

        return "Success";

    }
}
