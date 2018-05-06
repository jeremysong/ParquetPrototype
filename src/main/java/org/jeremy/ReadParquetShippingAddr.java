package org.jeremy;

import static org.apache.spark.sql.functions.col;
import static org.jeremy.ReadJsonShippingAddr.getSchema;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author yanson
 */
public class ReadParquetShippingAddr {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Read Parquet file")
                .master("local")
                .getOrCreate();

        SQLContext sqlContext = spark.sqlContext();

//        StructType schema = sqlContext.read().parquet("file:///Users/yanson/IdeaProjects/shipping_addr/parquet/slices/*").schema();

        Stopwatch sw = Stopwatch.createStarted();
        long total = sqlContext.read()
//                .schema(schema)
                .schema(getSchema())
                .parquet("file:///Users/yanson/IdeaProjects/shipping_addr/parquet/slices/*")
                .filter(col("data.postalCode").startsWith("98")).count();
        sw.stop();
        System.out.println("Parquet count:" + total + " with time: " + sw.elapsed(TimeUnit.SECONDS) + " seconds");

        spark.close();
    }

}
