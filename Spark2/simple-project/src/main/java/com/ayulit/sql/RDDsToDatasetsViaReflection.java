package com.ayulit.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;


/**
 * Spark SQL supports automatically converting an RDD of JavaBeans into a DataFrame.
 * The BeanInfo, obtained using reflection, defines the schema of the table.
 * Currently, Spark SQL does not support JavaBeans that contain Map field(s).
 * Nested JavaBeans and List or Array fields are supported though.
 * You can create a JavaBean by creating a class that implements Serializable and
 * has getters and setters for all of its fields.
 * 
 * */
public class RDDsToDatasetsViaReflection {

    public static void main(String[] args) {

        String appName = "RDDsToDatasets";
        
        String path = "src/main/resources/people.txt";
        
        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        
        // Create an RDD of Person objects from a text file
        JavaRDD<Person> peopleRDD = spark.read()
          .textFile(path)
          .javaRDD()
          .map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
              String[] parts = line.split(",");
              Person person = new Person();
              person.setName(parts[0]);
              person.setAge(Integer.parseInt(parts[1].trim()));
              return person;
            }
          });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");        
        
        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        
        // The columns of a row in the result can be accessed by field index ...
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
                                                                      @Override
                                                                      public String call(Row row) throws Exception {
                                                                        return "NameA: " + row.getString(0);
                                                                      }
                                                                     },
                                                                  stringEncoder);
        teenagerNamesByIndexDF.show();
        
        // or by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
                                                                      @Override
                                                                      public String call(Row row) throws Exception {
                                                                        return "NameB: " + row.<String>getAs("name");
                                                                      }
                                                                    },
                                                                 stringEncoder);
        
        teenagerNamesByFieldDF.show();
        
    }

}
