A = LOAD '$G' USING PigStorage(',') AS (key:long,value:long);
B = group A by key;
C = foreach B generate group,COUNT(A);
D = group C by $1;
O = foreach D generate group,COUNT($1);
STORE O INTO '$O' USING PigStorage (',');