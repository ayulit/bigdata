package ru.mera.practice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Create table in *.avro using snappy codec
 * 
 * */
public class Task03 {

    public static void main(String[] args) {

        // Should be some file on your system in 'resources'
        String file = "src/main/resources/people.json";
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        // Configuration to use snappy compression
        spark.conf().set("spark.sql.avro.compression", "snappy");        
        
        // Creating DataFrame from avro
//        Dataset<Row> df = spark.read().format("com.databricks.spark.avro").load("src/main/resources/autos.avro");        
        
        // Creating DataFrame from JSON
        Dataset<Row> df = spark.read().json(file);
        
        // Displays the content of the DataFrame to stdout
        df.show(); 
        
        // Writing out to avro
        df.write().format("com.databricks.spark.avro").save("output/");
        
        System.out.println("Done.");
        
    }

}
