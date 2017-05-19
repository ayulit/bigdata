package ru.mera.practice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Create table in *.avro using snappy codec
 * 
 * */
public class Task03 {

    public static void main(String[] args) {

        String appName = Task03.class.getSimpleName();

        // Should be some file on your system in 'resources'
        String file = "src/main/resources/people.json";
        
        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .getOrCreate();

        // Creating DataFrame from avro
        Dataset<Row> df = spark.read().format("com.databricks.spark.avro").load("src/main/resources/autos.avro");        
        
        // Creating DataFrame from JSON
//        Dataset<Row> df = spark.read().json(file);
        
        // Displays the content of the DataFrame to stdout
        df.show(); 

        // Saves the subset of the Avro records read in
        df.filter("modelYear > 1980").write().format("com.databricks.spark.avro").save("output/");
        
        System.out.println("Done.");
        
    }

}
