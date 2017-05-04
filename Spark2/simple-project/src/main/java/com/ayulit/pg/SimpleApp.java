package com.ayulit.pg;

import org.apache.spark.SparkConf;
/* SimpleApp.java */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class SimpleApp {
  public static void main(String[] args) {

      // Should be some file on your system in 'resources'
      String logFile = "src/main/resources/README.md";

      // Initializing Spark
      SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      // External Datasets
      JavaRDD<String> lines = sc.textFile(logFile); // just a pointer to the file

      // Sum sizes of all the lines using the map and reduce operations
      // map() is a transformation, returns new RDD
      // reduce() is an action, aggregates elements of the RDD
      JavaRDD<Integer> lineLengths = lines.map(s -> s.length()); // is not immediately computed, due to laziness!
      
      // this is optional (and a little bit slow...)
      lineLengths.persist(StorageLevel.MEMORY_ONLY()); // to be saved in memory after the first time it is computed.
      
      int totalLength = lineLengths.reduce((a, b) -> a + b); // breaks and run on separate machines

      System.out.println("result=" + totalLength);

      sc.stop();
      
  }
}