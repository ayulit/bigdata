package com.ayulit.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

//col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;

public class DataFrames {

    public static void main(String[] args) throws AnalysisException {

        String appName = "CreatingDataFrames example";
        
        // Should be some file on your system in 'resources'
        String file = "src/main/resources/people.json";
        
        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        
        // Creating DataFrame from JSON
        Dataset<Row> df = spark.read().json(file);

        // Displays the content of the DataFrame to stdout
//        df.show();

        
/* Untyped Dataset Operations (aka DataFrame Operations) */

        
        // Print the schema in a tree format
//        df.printSchema();

        // Select only the "name" column
//        df.select("name").show();
 
        // Select everybody, but increment the age by 1
//        df.select(col("name"), col("age").plus(1)).show();
        
        // Select people older than 26
//        df.filter(col("age").gt(26)).show();
        
        // Count people by age
//        df.groupBy("age").count().show();
        
/* Running SQL Queries Programmatically */
        
        // Register the DataFrame as a SQL temporary view
//        df.createOrReplaceTempView("people");
        
//        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        
//        sqlDF.show();
        
/* Global Temporary View */
        
        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();
        
        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        
        sc.close();

    }

}
