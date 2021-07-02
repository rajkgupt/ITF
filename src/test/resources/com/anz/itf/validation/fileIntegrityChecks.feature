#@ignore
Feature: File Integrity checks
  These checks are typically performed to ensure that the file
  received from a source system is wholly complete.


  #@ignore
  Scenario: Valid file – all checks are successful
    Given I have a DATA file named "scenarios/aus-capitals.csv"
    And I have a TAG file named "scenarios/aus-capitals.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-1.csv"
    Then the program should exit with RETURN CODE of 0


  #@ignore
  Scenario: Invalid file – record count does not match
    Given I have a DATA file named "scenarios/aus-capitals.csv"
    And I have a TAG file named "scenarios/aus-capitals-invalid-1.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-2.csv"
    Then the program should exit with RETURN CODE of 1

  #@ignore
  Scenario: Invalid file – file name does not match
    Given I have a DATA file named "scenarios/aus-capitals.csv"
    And I have a TAG file named "scenarios/aus-capitals-invalid-2.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-3.csv"
    Then the program should exit with RETURN CODE of 2

  #@ignore
  Scenario: Invalid file – primary key test fail
    Given I have a DATA file named "scenarios/aus-capitals-dupes.csv"
    And I have a TAG file named "scenarios/aus-capitals-dupes.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-4.csv"
    Then the program should exit with RETURN CODE of 3

  #@ignore
  Scenario: Invalid file – missing columns
    Given I have a DATA file named "scenarios/aus-capitals-missing.csv"
    And I have a TAG file named "scenarios/aus-capitals-missing.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-5.csv"
    Then the program should exit with RETURN CODE of 4

  #@ignore
  Scenario: Invalid file – additional columns
    Given I have a DATA file named "scenarios/aus-capitals-addition.csv"
    And I have a TAG file named "scenarios/aus-capitals-addition.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-6.csv"
    Then the program should exit with RETURN CODE of 4

