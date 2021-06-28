package com.anz.itf.validation;

import com.anz.itf.utils.ParseOptions;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.regex.Pattern;


public final class RunValidations {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

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

        spark.stop();
    }
}
