package org.jeremy;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;

public class AvroParquetS3Reader {
    public static void main(String[] args) throws IOException {

        String accessKey = "AKIAJF2UV6RZPUJY5JPA";
        String secretKey = "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";


//        AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentialsProvider(accessKey, secretKey));

        Configuration configuration = new Configuration();
        configuration.set("fs.s3a.access.key", accessKey);
        configuration.set("fs.s3a.secret.key", secretKey);

        String s3Object = "s3a://jeremy-song-test/parquet/person.parquet";
        String bucket = "jeremy-song-test";
        String key = "parquet/person.parquet";

        ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(HadoopInputFile.fromPath(new Path(s3Object), configuration))
                .withDataModel(GenericData.get())
                .build();

        GenericData.Record readRecord1 = reader.read();
        System.out.println(readRecord1.get("age"));
        System.out.println(readRecord1.get("pets"));

        System.out.println(reader.read());
    }

}
