package ru.mera.practice;


import java.io.Serializable;
import java.util.Arrays;
import java.sql.Date;
import java.util.GregorianCalendar;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 *  Avro to Parquet
 * 
 * */
public class Task04 {

    public static class Person implements Serializable {

        private static final long serialVersionUID = 1L;
        
        private int id;
        private String firstName;
        private String lastName;
        private Date date;
        
        public int getId() {
            return id;
        }
        public void setId(int id) {
            this.id = id;
        }
        public String getFirstName() {
            return firstName;
        }
        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }
        public String getLastName() {
            return lastName;
        }
        public void setLastName(String lastName) {
            this.lastName = lastName;
        }
        public Date getDate() {
            return date;
        }
        public void setDate(Date date) {
            this.date = date;
        }
    }    
    
    public static void main(String[] args) {

        // Should be some file on your system in 'resources'
        String file = "src/main/resources/people.json";        
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        // Create a few instances of a Bean class
        Person boss = new Person();
        boss.setId(0);
        boss.setFirstName("Hubert");
        boss.setLastName("Farnswoth");
        boss.setDate(Date.valueOf("2841-04-09"));
        
        Person deliveryboy = new Person();
        deliveryboy.setId(1);
        deliveryboy.setFirstName("Philip J.");
        deliveryboy.setLastName("Fry");
        deliveryboy.setDate(Date.valueOf("1974-08-14"));
        
        
        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
                
        // Creating DataSet
        Dataset<Person> ds = spark
                                .createDataset(Arrays.asList(boss,deliveryboy),
                                               personEncoder);
        
        // DataFrame from DataSet with rearranging and renaming columns (with Date cast to String)
        Dataset<Row> df = ds.selectExpr("id","firstName","lastName","cast(date as string) date")
                            .withColumnRenamed("id", "Id")
                            .withColumnRenamed("firstName", "First_Name")
                            .withColumnRenamed("lastName", "Last_Name")
                            .withColumnRenamed("date", "Date");
        

        
        
        // Displays the content of the DataFrame to stdout
        df.show();         

        // Writing out DataFrame to avro
        df.write().format("com.databricks.spark.avro").save("output/");
        
        System.out.println("Done.");
        
    }

}
