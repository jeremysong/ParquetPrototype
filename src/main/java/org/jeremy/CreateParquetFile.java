package org.jeremy;


import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.UUID;

public class CreateParquetFile {

    public static void main(String[] args) throws IOException {

        Schema schema = ReflectData.get().getSchema(Person.class);

        System.out.println(schema);

        UUID uuid = UUID.randomUUID();

        String filename = "file:///Users/jeremy/person-" + uuid.toString() + ".parquet";

        ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(new Path(filename))
                .withConf(new Configuration())
//                .withCompressionCodec(CompressionCodecName.GZIP)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withSchema(schema)
                .build();

        GenericData.Record record1 = new GenericData.Record(schema);
        record1.put("name", "Jeremy");
        record1.put("age", 28);
        record1.put("pets", Lists.newArrayList("Tom", "Jerry"));

        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("name", "Claire");
        record2.put("age", 27);
        record2.put("pets", Lists.newArrayList("Micky"));

        writer.write(record1);
        writer.write(record2);

        writer.close();

        ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(HadoopInputFile.fromPath(new Path(filename), new Configuration()))
                .withDataModel(GenericData.get())
                .build();

        GenericData.Record readRecord1 = reader.read();
        System.out.println(readRecord1.get("age"));
        System.out.println(readRecord1.get("pets"));

        System.out.println(reader.read());


    }
}
