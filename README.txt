The SQL compiler is designed using pyparser in Python.
The compiler takes the query input to transform it into relational algebra.
The structure of the compiler is as follows:
a.	SQL parser: SQL query is given as input
b.	Relational algebra conversion


SQL parser will have grammar, which is a crucial part of project,  to which the parser matches the query and identifies
the keywords and extracts column names, table names, where condition etc. 