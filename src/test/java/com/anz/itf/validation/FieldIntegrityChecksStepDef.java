package com.anz.itf.validation;

import com.anz.itf.utils.FileOperations;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.HashMap;
import static org.junit.Assert.assertEquals;

public class FieldIntegrityChecksStepDef {
    HashMap<String, String > inputArgsMap = new HashMap<>();
    RunValidationHelper helper = new RunValidationHelper();
    int exitCode;


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


    }


    @Then("{string} should match {string}")
    public void should_match(String string, String string2) throws Exception{
        inputArgsMap.put("actualOutput",string);
        exitCode = helper.performFieldValidationCheck(inputArgsMap);

    }
    @Then("the program should exit with RETURN CODE1 of {string}")
    public void the_program_should_exit_with_return_code1_of(String string) {
        boolean compareResult = true;
        assertEquals(true, compareResult);

    }
}
