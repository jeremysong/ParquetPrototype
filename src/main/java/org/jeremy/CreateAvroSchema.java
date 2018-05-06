package org.jeremy;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class CreateAvroSchema {
    public static void main(String[] args) {
        Schema schema = SchemaBuilder
                .record("Person")
                .fields()
                .requiredString("name")
                .name("pets")
                .type(Schema.createArray(Schema.create(Schema.Type.STRING)))
                .noDefault()
                .requiredInt("age")
                .endRecord();

        System.out.println(schema);
    }
}
