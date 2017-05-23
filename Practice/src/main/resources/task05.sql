--creating external table for complex data Array, Map, Struct
CREATE EXTERNAL TABLE hive_complex_table (`first` array<string>, `second` map<int,string>, `third` struct<num:Int,str:String>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' collection items terminated BY '&' map KEYS terminated BY '#' LINES TERMINATED BY '\n' stored AS textfile LOCATION '/xlitand/vault/practice/task05/';

--selecting Array values
SELECT hive_complex_table.first[0] FROM hive_complex_table;
SELECT hive_complex_table.first[1] FROM hive_complex_table;

--selecting Map values 
SELECT hive_complex_table.second[101] FROM hive_complex_table WHERE second[101] IS NOT null;

--selecting Struct values
SELECT hive_complex_table.third.num FROM hive_complex_table;
SELECT hive_complex_table.third.str FROM hive_complex_table;
