package com.anz.itf.validation;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;

public class FileIntegrityChecksStepDef {
    HashMap<String, String > inputArgsMap = new HashMap<>();
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
        RunValidationsMain main = new RunValidationsMain();
        exitCode = main.runValidations(inputArgsMap);
    }

    @Then("the program should exit with RETURN CODE of {int}")
    public void the_program_should_exit_with_return_code_of(int val) {
        assertEquals(val,exitCode);
    }
}
