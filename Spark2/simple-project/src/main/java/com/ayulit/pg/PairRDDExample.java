package com.ayulit.pg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRDDExample {

    public static void main(String[] args) {

        // Should be some file on your system in 'resources'
        String logFile = "src/main/resources/PairRDDExample.txt";

        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName("PairRDDExample").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // External Datasets
        JavaRDD<String> lines = sc.textFile(logFile); // just a pointer to the file        
        
        /* The following code uses the reduceByKey operation on key-value pairs
         * to count how many times each line of text occurs in a file */
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        // Printing tuples
        counts.take(100).forEach(tuple -> System.out.println(tuple));
    }

}
