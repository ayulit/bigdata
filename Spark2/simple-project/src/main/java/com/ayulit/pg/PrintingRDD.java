package com.ayulit.pg;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PrintingRDD {

    public static void main(String[] args) {

        // Input data
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName("PrintingRDD").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Dataset from List
        JavaRDD<Integer> rdd = sc.parallelize(data);
        
        // Wrong: Don't do this in cluster mode!!
        // rdd.foreach(x -> System.out.println(x));
        
        // Not safe for printing: using collector instead can cause out of memory!
        // rdd.collect().forEach(x -> System.out.println(x));
        
        // Safer approach for printing a few elements
        rdd.take(100).forEach(x -> System.out.println(x));

    }

}
