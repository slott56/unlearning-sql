Feature: Loads activation records from an external source
Scenario: Loads a file with a mixture of good and bad records
  Given a file of activations
      | customer_name | device_name | service_name | start_date | latitude | longitude |
      | customer x | device y | service z | 2022-07-10T11:12:13+00:00 | 35°21.2833′N | 082°31.6333′W |
      | | device y | service z | 2022-07-10T11:12:13+00:00 | 35°21.2833′N | 082°31.6333′W |
  And a working test database
  And the database has matching customers
      | customeer_name |
      | customer x |
  And the database has matching customer-device ownership
      | customer_name | device_name |
      | customer x | device y |
  And the database has matching service names
      | service_name |
      | service z |
  When the python_load_process.py application is run
  Then the 1 record(s) with valid data are loaded
  And the 1 record(s) with invalid data are ignored
  And the output file has 1 valid record
