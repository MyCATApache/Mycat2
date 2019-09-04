-
SELECT CAST("2018-12-30" AS DATE);
|CAST('2018-12-30' AS DATE)|
|2018-12-30|
-
SELECT 1+1,(1+100)*2/8/(1.0+CAST((1/2) as DOUBLE));
|1 + 1|(1 + 100) * 2 / 8 / (1.0 + CAST(1 / 2 AS DOUBLE))|
|2|16.833333333333332|
-
SELECT current_date();
|current_date()|
|2019-08-29|
-
CREATE DATABASE db1;
ok
-
show databases;
|Database|
|db1|
-