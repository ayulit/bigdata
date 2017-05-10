package com.ayulit.sql;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class Datasets {
    
    public static void main(String[] args) {

        String appName = "Datasets";
        
        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        
        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        
        // And finally, Dataset creation
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
                );
        
        javaBeanDS.show();
        
        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
 
        Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {
                                                                @Override
                                                                public Integer call(Integer value) throws Exception {
                                                                    return value + 1;
                                                                }
                                                             },
                                                         integerEncoder);

        // Returns [2, 3, 4]
        transformedDS.show();

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
        
    }

}
