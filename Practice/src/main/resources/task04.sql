--do this if necessary
CREATE DATABASE persondb;
USE persondb;

--creating external table for avro 
CREATE EXTERNAL TABLE person ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' LOCATION '/xlitand/vault/practice/person' TBLPROPERTIES ( 'avro.schema.url'='hdfs:///xlitand/vault/practice/person_schema/person.avsc');

--creating table for parquet with dynamic partitioning
CREATE EXTERNAL TABLE parquet_dyn (`id` int, `first_name` string, `last_name` string) PARTITIONED BY (`date` String) STORED AS PARQUET LOCATION '/xlitand/vault/practice/person_parquet_dyn';

--data will be copied from avro to parquet with dynamic partitioning by `date`
INSERT OVERWRITE TABLE parquet_dyn PARTITION (`date`) SELECT `id`,`first_name`,`last_name`,`date` FROM person;