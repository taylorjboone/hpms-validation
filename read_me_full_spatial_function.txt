Created 6/14/23
Currently the file reads a csv known as "better_test_data.csv" that needs
to be in the same folder as the code. 
The code reads in the file and applies each function to the data.
Currently the code only appends a column to the data frame in memory with
the column header being named the Full Spatial Join rule being check and whether
data passed or fail. data passes the rule if the rule returns False otherwise if
the column returns True then the data is in accurate. the test data called
"better_test_data.csv" has a column called "pass_or_fail" which tells what rows
should always fail and what rows should always pass.