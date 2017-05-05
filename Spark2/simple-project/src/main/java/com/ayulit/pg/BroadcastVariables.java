package com.ayulit.pg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastVariables {

    public static void main(String[] args) {

        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName("BroadcastVariables").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        String v = "variable";
        
        // Broadcast variable - a read-only variable cached on each machine
        // after broadcasting 'v' should not be modified!
        Broadcast<String> broadcastVar = sc.broadcast(v); // warpper for 'v' makes it broadcast

        System.out.println(broadcastVar.value()); // returns "variable"

    }

}
