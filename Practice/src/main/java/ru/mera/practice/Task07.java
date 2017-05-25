package ru.mera.practice;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Task07 {

    public static void main(String[] args) {
        
        // Should be some file on your system in 'resources'
        String logFile = "src/main/resources/task07.txt";

        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName("Task07").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // External datasets
        JavaRDD<String> lines = sc.textFile(logFile); // just a pointer to the file        
        
        // classical wordcount, nothing new there
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> tuples = pairs.reduceByKey((a, b) -> a + b);
        
        // to enable sorting by value (count) and not key -> value-to-key conversion pattern
        JavaPairRDD<Integer, String> newTuples = tuples.mapToPair(tuple -> new Tuple2<Integer, String>(tuple._2, tuple._1));

        // sorting
        JavaPairRDD<Integer, String> sortedTuples = newTuples.sortByKey(false);
        
        // printing
        sortedTuples.take(10).forEach(tuple -> System.out.println(tuple));        
        
        sc.close();
        
        System.out.println("Done.");

    }
    
}


