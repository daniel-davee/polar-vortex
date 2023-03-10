@polars @database
Feature:  Polars Database Interfaces

    This will be a interfaces for the Polars dataframes to be used with CSV or Parquet.

  Background: Setup intial objects
    Given the variable value is foo:420, bar:69
     And the variable index is None
     And the variable database is behave
     And the variable key is test
     And the log level is debug

    Scenario: Create database and and key_file 
      Given database and key_file should not exist
       When run create
       And run key_file
        Then True should be returned
      When run empty
        Then True should be returned
    
    Scenario: Upsert a row 
      When run upsert
        Then True should be returned
    
    Scenario: Save data 
      When run save
        Then all values should be the result

    Scenario: Get all the data 
      When run get
       Then all values should be the result 
    
    Scenario: Get an index 
      When run upsert
      And run get
       Then value at 1 should be returned

    Scenario: Get a value
     When run upsert
     And run get
      Then value should be returned

    Scenario: Try to delete with lock
    Given the variable value is foo:420, bar:69
    Given the variable locked is True
       When run delete
        Then False should be returned