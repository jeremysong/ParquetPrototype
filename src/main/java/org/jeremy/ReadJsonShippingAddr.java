package org.jeremy;

import static org.apache.spark.sql.functions.col;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author yanson
 */
public class ReadJsonShippingAddr {
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Read JSON file")
                .master("local")
                .getOrCreate();

        SQLContext sqlContext = spark.sqlContext();

//        StructType schema = sqlContext.read().json("file:///Users/yanson/IdeaProjects/shipping_addr/json/slices/*").schema();
        
        Stopwatch sw = Stopwatch.createStarted();
        long total = sqlContext.read()
                // need to provide schema to speed up the process
//                .schema(schema)
                .schema(getSchema())
                .json("file:///Users/yanson/IdeaProjects/shipping_addr/json/slices/*")
                .filter(col("data.postalCode").startsWith("98")).count();
        sw.stop();
        System.out.println("JSON count:" + total + " with time: " + sw.elapsed(TimeUnit.SECONDS) + " seconds");
        
        spark.close();
    }
    
    public static StructType getSchema() {
        StructField customerId = DataTypes.createStructField("customerId", DataTypes.LongType, false);
        StructField eventTime = DataTypes.createStructField("eventTime", DataTypes.LongType, false);
        StructField countryCode = DataTypes.createStructField("countryCode", DataTypes.StringType, false);
        StructField postalCode = DataTypes.createStructField("postalCode", DataTypes.StringType, false);
        StructField stateCode = DataTypes.createStructField("stateCode", DataTypes.StringType, true);
        StructField city = DataTypes.createStructField("city", DataTypes.StringType, true);
        StructField marketplaceId = DataTypes.createStructField("marketplaceId", DataTypes.LongType, false);
        StructField addressId = DataTypes.createStructField("addressId", DataTypes.LongType, false);
        
        StructField data = DataTypes.createStructField("data", DataTypes.createStructType(
                Lists.newArrayList(customerId, eventTime, countryCode, postalCode, stateCode, city, marketplaceId, addressId)), false);
        StructField operation = DataTypes.createStructField("operation", DataTypes.StringType, false);
        StructField uploadedDate = DataTypes.createStructField("uploadedDate", DataTypes.LongType, false);
        
        return DataTypes.createStructType(Lists.newArrayList(data, operation, uploadedDate));
    }

}
