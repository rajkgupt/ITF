package com.anz.itf.validation;

import com.anz.itf.utils.FileOperations;
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
import org.apache.spark.sql.functions;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class FieldIntegrityChecksStepDef {
    HashMap<String, String > inputArgsMap = new HashMap<>();
    RunValidationHelper helper = new RunValidationHelper();
    SparkSession spark = null;
    Dataset<Row> inputFileDF = null;

    @Given("I have a DATA named {string}")
    public void i_have_a_data_named(String string) {
        inputArgsMap.put("data",string);

    }

    @Given("I have a TAG file named1 {string}")
    public void i_have_a_tag_file_named1(String string) {
        inputArgsMap.put("tag",string);
    }

    @Given("I have a SCHEMA file named1 {string}")
    public void i_have_a_schema_file_named1(String string) {
        inputArgsMap.put("schema",string);
    }

    @When("I execute the application with output1 {string}")
    public void i_execute_the_application_with_output1(String string) throws Exception{
        inputArgsMap.put("output", string);

        spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("FieldIntegrityChecks")
                .getOrCreate();

        inputFileDF = helper.getDataFrame(spark, inputArgsMap);





    }

    @Then("the program should exit with RETURN CODE1 of {string}")
    public void the_program_should_exit_with_return_code1_of(String string) {

    }
    @Then("{string} should match {string}")
    public void should_match(String string, String string2) throws Exception{

        inputArgsMap.put("ExpectedOutput",string2);


        Dataset<Row>newDfWithDirtyFlag = helper.getDataFrameWithDirtyFlag(spark, inputFileDF, inputArgsMap);
        //String returnMessage = helper.writeDataFrame(spark, newDfWithDirtyFlag, inputArgsMap);

        FileOperations fileOperations = new FileOperations();
        boolean compareResult = fileOperations.compareTwoFiles(inputArgsMap.get("output"),inputArgsMap.get("ExpectedOutput") );

        assertEquals(true, compareResult);
    }

}
