package com.anz.itf;

import io.cucumber.junit.CucumberOptions;
import io.cucumber.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(tags = "not @ignore",plugin = {"pretty", "html:target/cucumber-report.html"})
public class TestRunner {
}
