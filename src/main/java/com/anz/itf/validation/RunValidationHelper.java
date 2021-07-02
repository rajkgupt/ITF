package com.anz.itf.validation;

import com.anz.itf.utils.FileOperations;
import org.apache.spark.sql.*;
import java.util.HashMap;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

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
                .option("enforceSchema",true)
                .load(inputArgsMap.get("data")).cache();

        return inputFileDF;
    }



    /**
     * Performs record Count checks
     * @param inputFileDF
     * @param inputArgsMap
     * @return 0 if records Count match other returns 1
     */
    public int performRecordCountCheck(Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

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
    public int performFileNameCheck(Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));

        //Operation for fileName check
        fileNameWithFolderFromInputCommand = inputArgsMap.get("data");

        Path path = Paths.get(fileNameWithFolderFromInputCommand);
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
    public int performPrimaryKeyCount(SparkSession spark,
                                      Dataset<Row> inputFileDF, HashMap<String, String> inputArgsMap) {

        long countBeforeDroppingDupes = inputFileDF.count();

        String primaryKeysArray[] = { "State/Territory"}; //TODO take from schema DS

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
    public int performMissingOrAdditionalColumnCount(SparkSession spark,
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

    /**
     *
     * @param inputArgsMap
     * @return
     */
    public int performFieldValidationCheck(HashMap<String, String> inputArgsMap) throws Exception{

        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("RunValidations locally")
                .getOrCreate();


        Dataset<Row> inputFileDF = getDataFrame(spark, inputArgsMap);

        Dataset<Row>newDf = inputFileDF.withColumn("dirty_flag", functions.when(functions.col("_corrupt_record").isNotNull(), 1)
                .otherwise(0));
        newDf = newDf.drop("_corrupt_record");
        newDf.show();

        //comment in windows due to winutils issue
        /*
        newDf.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .option("dateFormat", "dd-MM-yyyy")
                .option("nullValue", "") //\u0000 is printing ""...find another way..
                .option("quoteAll", "false")
                .csv(inputArgsMap.get("actualOutput"));
        */

        FileOperations fileOps = new FileOperations();
        fileOps.moveWrittenFileToTopLevel(spark,newDf,inputArgsMap);



        //TODO - implement file compare function
        FileOperations fileOperations = new FileOperations();
        //boolean compareResult = fileOperations.compareTwoFiles(inputArgsMap.get("output"),inputArgsMap.get("actualOutput") );
        boolean compareResult = true;
        if (compareResult)
            return 0;
        else
            return 1;
    }



}
