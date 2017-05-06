package com.ayulit.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrames {

    public static void main(String[] args) {

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
        df.show();

        
        sc.close();

    }

}
