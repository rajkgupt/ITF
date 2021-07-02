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
    int exitCode=0;

    public int runValidations(HashMap<String, String> inputArgsMap) {

        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("RunValidations locally")
                .getOrCreate();

        RunValidationHelper helper = new RunValidationHelper();
        Dataset<Row> inputFileDF = helper.getDataFrame(spark, inputArgsMap);

        if (exitCode == 0) exitCode = helper.performRecordCountCheck(inputFileDF, inputArgsMap);
        if (exitCode == 0) exitCode = helper.performFileNameCheck(inputFileDF, inputArgsMap);
        if (exitCode == 0) exitCode = helper.performPrimaryKeyCount(spark, inputFileDF, inputArgsMap);
        if (exitCode == 0) exitCode = helper.performMissingOrAdditionalColumnCount(spark, inputFileDF, inputArgsMap);

        spark.stop();

        return exitCode;
    }
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

        main.exitCode = main.runValidations(inputArgsMap);
        System.exit(main.exitCode);
    }
}
