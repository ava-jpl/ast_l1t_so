## AST_L1T-SO
Generates AST_L1T-SO Products from AST_L1T Products
----
There is 1 associated jobs:
- Generate - AST_L1T-SO

### Generate - AST_L1T_SO
-----
Job is of type iteration. It takes in an input AST_L1T product. It localizes the AST_L1T and generates a band difference product that shows b10 + b12 - 2 * b11. This is a quick proxy for identifying plumes and SO2 emissions.

L1T_SO product spec is the followingc:

    AST_L1T-SO-<sensing_start_datetime>_<sensing_end_datetime>-<version_number>
