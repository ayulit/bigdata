package com.ayulit.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSources {

    public static void main(String[] args) {
        
        String path = "src/main/resources/users.parquet";

        String appName = "DataSources";
        
        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        
        // Generic Load Function
        Dataset<Row> usersDF = spark.read().load(path);
        
        // Generic Save Function - doesn't work on Windows ?!
//        usersDF.select("name", "favorite_color").write().format("parquet").save("src/main/resources/namesAndFavColors.parquet");

    }

}
