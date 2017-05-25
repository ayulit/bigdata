package ru.mera.practice;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class Task07 {
    
    // It's useful to define a sum reducer -
    // this is a function that takes in two integers and returns their sum
    private static Function2<Integer, Integer, Integer> SUM_REDUCER = (a, b) -> a + b;
    
    private static class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable {

        private static final long serialVersionUID = 1L;

        private Comparator<V> comparator;

        public ValueComparator(Comparator<V> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Tuple2<K, V> t1, Tuple2<K, V> t2) {
            // values of tuples comparison
            return comparator.compare(t1._2(), t2._2());
        }
    }

    public static void main(String[] args) {
        
        // Should be some file on system in 'resources'
        String logFile = "src/main/resources/task07.txt";

        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Task07").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // Load the text file into Spark.
        JavaRDD<String> logLines = sc.textFile(logFile); // just a pointer to the file        
        
        /* Code for processing logs. */
        
        // Convert the text log lines to LoginAssignmentLog objects and
        // cache them since multiple transformations and actions will be called on the data.
        JavaRDD<LoginAssignmentLog> accessLogs =
                logLines.map(LoginAssignmentLog::parseFromLogLine).cache();        
        
        // Computing the top ten logins according to their frequency
        List<Tuple2<String, Integer>> topLogins = accessLogs
                .mapToPair(log -> new Tuple2<>(log.getUserID(), 1))
                .reduceByKey(SUM_REDUCER)
                .top(10, new ValueComparator<>(Comparator.<Integer>naturalOrder()));
        
        System.out.println("Top Logins: " + topLogins);

        sc.close();
        
        System.out.println("Done.");

    }
    
}


