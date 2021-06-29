#@ignore
Feature: Field (Data) Integrity checks
  These tests are typically performed to test the quality of the data

  @ignore
  Scenario: Valid fields
    Given I have a DATA named "scenarios/aus-capitals.csv"
    And I have a TAG file named1 "scenarios/aus-capitals.tag"
    And I have a SCHEMA file named1 "scenarios/aus-capitals.json"
    When I execute the application with output1 "output/act-sbe2-1.csv"
    Then the program should exit with RETURN CODE of 0
    And "output/act-sbe2-1.csv" should match "scenarios/exp-sbe2-1.csv"

  #@ignore
  Scenario: Invalid fields
    Given I have a DATA named "scenarios/aus-capitals-invalid-3.csv"
    And I have a TAG file named1 "scenarios/aus-capitals.tag"
    And I have a SCHEMA file named1 "scenarios/aus-capitals.json"
    When I execute the application with output1 "output/act-sbe2-2.csv"
    Then the program should exit with RETURN CODE of 0
    And "output/act-sbe2-2.csv" should match "scenarios/exp-sbe2-2.csv"