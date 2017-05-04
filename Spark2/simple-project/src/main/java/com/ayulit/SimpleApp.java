package com.ayulit;

/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
  public static void main(String[] args) {
	
	// Should be some file on your system  
    String logFile = "/home/andrey/BigData/spark-2.1.0-bin-hadoop2.4/" + "README.md";
    
    // Initializing Spark
    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
    sc.stop();
  }
}