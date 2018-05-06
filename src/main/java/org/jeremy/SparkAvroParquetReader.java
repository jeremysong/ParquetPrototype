package org.jeremy;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkAvroParquetReader {
    public static void main(String[] args) {

        String accessKey = "AKIAJF2UV6RZPUJY5JPA";
        String secretKey = "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";

        SparkConf configuration = new SparkConf();
        configuration.set("fs.s3a.access.key", accessKey);
        configuration.set("fs.s3a.secret.key", secretKey);

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Avro Parquet Reader")
                .master("local")
                .config(configuration)
                .getOrCreate();

//        SparkContext context = spark.sparkContext();
        SQLContext sqlContext = spark.sqlContext();

        Dataset<Row> dataSet = sqlContext.read().parquet("s3a://jeremy-song-test/parquet/person.parquet");
        dataSet.cache();
        dataSet.printSchema();

        dataSet.foreach((ForeachFunction<Row>) row -> System.out.println(row.getList(2)));
        dataSet.foreach((ForeachFunction<Row>) row -> System.out.println(row));

        spark.stop();
    }
}
