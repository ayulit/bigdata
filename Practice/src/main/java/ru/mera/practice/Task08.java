package ru.mera.practice;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Task08 {

    public static class ViceMax extends UserDefinedAggregateFunction {

        private static final long serialVersionUID = 1L;
        
        private StructType inputSchema;
        private StructType bufferSchema;

        public ViceMax() {
            
            List<StructField> inputFields = new ArrayList<>();
            
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            
            bufferFields.add(DataTypes.createStructField("max", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("secMax", DataTypes.LongType, true));
            
            bufferSchema = DataTypes.createStructType(bufferFields);
        }
        
        
        /* Just getters first */        
        // Data types of input arguments of this aggregate function
        @Override
        public StructType inputSchema() {
            return inputSchema;
        }        
        // Data types of values in the aggregation buffer
        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        // The data type of the returned value
        @Override
        public DataType dataType() {
            return DataTypes.LongType;
        }

        // Whether this function always returns the same output on the identical input
        @Override
        public boolean deterministic() {
            return true;
        }



        // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
        // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
        // the opportunity to update its values. Note that arrays and maps inside the buffer are still
        // immutable.
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            
            // we've ordered 2 buffers
            buffer.update(0, 0L); // for max
            buffer.update(1, 0L); // for secMax
        }

        // Updates the given aggregation buffer `buffer` with new input data from `input`
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {

            if (!input.isNullAt(0)) {
                
                long max = buffer.getLong(0);
                long secondMax = buffer.getLong(1);                
                long columnCurrentValue = input.getLong(0);

                if (columnCurrentValue > max) {
                    secondMax = max;
                    max = columnCurrentValue;
                } else if (columnCurrentValue > secondMax) {
                    secondMax = columnCurrentValue;
                }
                
                long updatedMax = max;
                long updatedSecondMax = secondMax;
                
                buffer.update(0, updatedMax);
                buffer.update(1, updatedSecondMax);
            }            
        }        
        
        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            
            long mergedMax = Math.max(buffer1.getLong(0),buffer2.getLong(0));
            long mergedSecondMax = Math.max(buffer1.getLong(1),buffer2.getLong(1));
            
            buffer1.update(0, mergedMax);
            buffer1.update(1, mergedSecondMax);
        }
        
        // Calculates the final result
        @Override
        public Object evaluate(Row buffer) {
            return buffer.getLong(1);
        }        

    }
    
    public static void main(String[] args) {

        // Should be some file on your system in 'resources'
        String file = "src/main/resources/products.json";
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        // Creating DataFrame from json, rearranging columns
        Dataset<Row> df = spark.read().json(file).selectExpr("Id","product_name","product_category","product_revenue");
        
        df.createOrReplaceTempView("products");
        
        // Displays the content of the DataFrame to stdout
//        df.show();         

        // Writing out DataFrame to avro
//        df.write().format("com.databricks.spark.avro").save("output1/");        

        // Register the function to access it
        spark.udf().register("viceMax", new ViceMax());
           
        String query = "SELECT p.Id,p.product_name,p.product_category,p.product_revenue, max(CASE WHEN maxpr = product_revenue THEN product_name END) OVER v AS mad_max, max(CASE WHEN 2dmaxpr = product_revenue THEN product_name END) OVER v AS mad_max2, maxpr-product_revenue AS delta FROM (SELECT *, max(product_revenue) OVER w AS maxpr, viceMax(product_revenue) OVER w AS 2dmaxpr FROM products WINDOW w AS (PARTITION BY product_category)) p WINDOW v AS (PARTITION BY product_category) ORDER by Id";
        
        Dataset<Row> result = spark.sql(query);
        
        result.show();
        
        // Writing out DataFrame to parquet
        result.coalesce(1).write().format("parquet").partitionBy("product_category").mode(SaveMode.Append).save("products.parquet");
        
        System.out.println("Done.");

    }

}
