package com.anz.itf.validation;
import com.anz.itf.utils.FileOperations;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;

public class FileIntegrityChecksStepDef {
    HashMap<String, String > inputArgsMap = new HashMap<>();
    HashMap<String, Long> recordCountCheckMap;
    HashMap<String, String> fileNameCheckMap;
    HashMap<String, Long> primaryKeyCountMap;
    HashMap<String, Long> missingOrAdditionalColumnCountMap;


    int exitCode;


    @Given("I have a DATA file named {string}")
    public void i_have_a_data_file_named(String string) {
        inputArgsMap.put("data",string);

    }
    @Given("I have a TAG file named {string}")
    public void i_have_a_tag_file_named(String string) {

        inputArgsMap.put("tag",string);
    }

    @Given("I have a SCHEMA file named {string}")
    public void i_have_a_schema_file_named(String string) {

        inputArgsMap.put("schema",string);
    }

    @When("I execute the application with output {string}")
    public void i_execute_the_application_with_output(String string) {
        inputArgsMap.put("output",string);

        inputArgsMap.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });


        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("FileIntegrityChecks")
                .getOrCreate();

        RunValidationHelper helper = new RunValidationHelper();
        Dataset<Row> inputFileDF = helper.getDataFrame(spark, inputArgsMap);

        recordCountCheckMap = helper.getRecordCountCheck(inputFileDF, inputArgsMap);

        fileNameCheckMap = helper.getFileNameCheck(inputFileDF, inputArgsMap);

        primaryKeyCountMap = helper.getPrimaryKeyCount(spark, inputFileDF, inputArgsMap);

        missingOrAdditionalColumnCountMap = helper.getMissingOrAdditionalColumnCount(spark, inputFileDF, inputArgsMap);

        spark.stop();

    }

    @Then("the program should exit with RETURN CODE of {string}")
    public void the_program_should_exit_with_return_code_of(String string) {


        assertEquals(fileNameCheckMap.get("fileNameWithoutFolderFromInputCommand"),
                fileNameCheckMap.get("dataFileNameFromTagFile")); //fileName check
        assertEquals(recordCountCheckMap.get("totalRecordCountFromDF"),
                recordCountCheckMap.get("recordCountFromTagFile")); //record count check
        assertEquals(0,(long)primaryKeyCountMap.get("primaryKeyCount")); //primary key count check

        //missing or additional column records check
        assertEquals(0,
                (long)missingOrAdditionalColumnCountMap.get("missingOrAdditionalColumnCount"));


        /*
        if (fileNameWithoutFolderFromInputCommand.equals(dataFileNameFromTagFile)) {
            if (totalRecordCountFromDF == recordCountFromTagFile) {
                if (primaryKeyCount == 0) {
                    if (missingOrAdditionalColumnCount == 0) {
                        assertTrue("0",true);
                    }
                }
            }
        }*/

    }
    @After("@tag1")
    public void testEnd(){
        if (exitCode == 0) {
            System.out.println("Exiting with status 0");
            //System.exit(0);
        } else {
            System.out.println("Exiting with status 1");
            //System.exit(11);
        }
    }
}
