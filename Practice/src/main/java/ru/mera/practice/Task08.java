package ru.mera.practice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Task08 {

    public static void main(String[] args) {

        // Should be some file on your system in 'resources'
        String file = "src/main/resources/products.json";
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        // Creating DataFrame from json, rearranging columns
        Dataset<Row> df = spark.read().json(file).selectExpr("Id","product_name","product_category","product_revenue");
        
        // Displays the content of the DataFrame to stdout
        df.show();         

        // Writing out DataFrame to avro
        df.write().format("com.databricks.spark.avro").save("output1/");        
        
        System.out.println("Done.");

    }

}
