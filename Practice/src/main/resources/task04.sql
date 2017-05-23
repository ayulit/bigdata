--do this if necessary
CREATE DATABASE persondb;
USE persondb;

--creating external table for avro 
CREATE EXTERNAL TABLE person ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' LOCATION '/xlitand/vault/practice/person' TBLPROPERTIES ( 'avro.schema.url'='hdfs:///xlitand/vault/practice/person_schema/person.avsc');

