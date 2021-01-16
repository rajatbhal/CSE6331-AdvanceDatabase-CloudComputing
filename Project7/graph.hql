DROP TABLE T1; 
CREATE TABLE T1(num1 int, num2 int )
row format delimited fields terminated by ',' stored as textfile;
load data local inpath '${hiveconf:G}' overwrite into table T1;
INSERT OVERWRITE TABLE T1
SELECT num1, COUNT(*) FROM T1 GROUP BY num1; SELECT num2,COUNT(*) FROM T1 GROUP BY num2;
