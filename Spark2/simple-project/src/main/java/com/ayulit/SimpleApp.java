package com.ayulit;

import org.apache.spark.SparkConf;
/* SimpleApp.java */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
  public static void main(String[] args) {

      // Should be some file on your system in 'resources'
      String logFile = "src/main/resources/README.md";

      // Initializing Spark
      SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      // External Datasets
      JavaRDD<String> logData = sc.textFile(logFile).cache();

      // Sum sizes of all the lines using the map and reduce operations
      Integer result = logData.map(s -> s.length()).reduce((a, b) ->  a + b);

      System.out.println("result=" + result);

      sc.stop();
      
  }
}