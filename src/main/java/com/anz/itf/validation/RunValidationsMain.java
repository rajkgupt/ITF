package com.anz.itf.validation;

import com.anz.itf.utils.ParseOptions;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.regex.Pattern;





public final class RunValidationsMain {
    private static final Pattern SPACE = Pattern.compile(" ");
    HashMap<String, String > inputArgsMap = new HashMap<>();
    HashMap<String, Long> recordCountCheckMap;
    HashMap<String, String> fileNameCheckMap;
    HashMap<String, Long> primaryKeyCountMap;
    HashMap<String, Long> missingOrAdditionalColumnCountMap;

    public static void main(String[] args) throws Exception {

        RunValidationsMain main = new RunValidationsMain();

        Options options = ParseOptions.OPTIONS;
        HashMap<String, String > inputArgsMap = null;

        if (args.length < 4) {
            ParseOptions.printHelp(options);
            System.exit(1);
        } else {
            inputArgsMap = ParseOptions.parseOptions(options, args);
        }

        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("RunValidations locally")
                .getOrCreate();


        RunValidationHelper helper = new RunValidationHelper();
        Dataset<Row> inputFileDF = helper.getDataFrame(spark, inputArgsMap);

        main.recordCountCheckMap = helper.getRecordCountCheck(inputFileDF, inputArgsMap);

        main.fileNameCheckMap = helper.getFileNameCheck(inputFileDF, inputArgsMap);

        main.primaryKeyCountMap = helper.getPrimaryKeyCount(spark, inputFileDF, inputArgsMap);

        main.missingOrAdditionalColumnCountMap = helper.getMissingOrAdditionalColumnCount(spark, inputFileDF, inputArgsMap);



        spark.stop();
    }
}
