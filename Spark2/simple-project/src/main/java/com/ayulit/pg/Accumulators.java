package com.ayulit.pg;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class Accumulators {

    public static void main(String[] args) {
        // Input data
        List<Integer> data = Arrays.asList(1, 2, 3, 4);

        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName("Accumulators").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Dataset from List
        JavaRDD<Integer> rdd = sc.parallelize(data);
        
        // Long value accumulator
        LongAccumulator accum = sc.sc().longAccumulator();
        
        System.out.println("accum =" + accum.value()); // '0' in the beginning
        
        // adding to the accumulator
        rdd.foreach(x -> accum.add(x));
        
        System.out.println("accum =" + accum.value()); // 10
        
        // newRdd is a sqaure of RDD
        JavaRDD<Integer> newRdd = rdd.map(x -> { accum.add(x); return f(x); }); // 1,4,9,16
        
        // Here, accum is still 10 because no actions have caused the `map` to be computed!
        System.out.println("accum =" + accum.value()); // 10
        
        // after reduce (action) we will have new accum
        int squaresSum = newRdd.reduce((a, b) -> a + b);
        System.out.println("accum =" + accum.value()); // 20
        
        System.out.println("squaresSum =" + squaresSum); // 30
        
        sc.close();
        
    }

    /** Just square of number  */
    private static Integer f(Integer x) {
        return x*x;
    }

}
