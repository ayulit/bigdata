package com.ayulit.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** The built-in DataFrames functions provide common aggregations such as count(),
 *  countDistinct(), avg(), max(), min(), etc.
 *  While those functions are designed for DataFrames,
 *  Spark SQL also has type-safe versions for some of them in Scala and Java
 *  to work with strongly typed Datasets.
 *   Moreover, users are not limited to the predefined aggregate functions and can create their own. */
public class AggregateFunctionsUntyped {

    public static class MyAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
            
          }

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
            return DataTypes.DoubleType;
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
            
            // 2 buffers ?!
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }        

        // Updates the given aggregation buffer `buffer` with new input data from `input`
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                long updatedCount = buffer.getLong(1) + 1;
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
              }
        }

        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergedSum);
            buffer1.update(1, mergedCount);
        }        
        
        // Calculates the final result: = summ / count
        @Override
        public Double evaluate(Row buffer) {
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
        
    }
    
    public static void main(String[] args) {

        String appName = "AggregateFunctionsUntyped";
        
        String path = "src/main/resources/employees.json";
        
        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // The entry point into Spark SQL functionality
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        
        // Register the function to access it
        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> df = spark.read().json(path);
        df.createOrReplaceTempView("employees");
        df.show();
        
        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();        
        
    }

}
