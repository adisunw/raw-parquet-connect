package io.confluent.connect.s3.format.parquet;


import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class BinaryWriteSupport<T> extends WriteSupport<T> {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;

    // TODO: support specifying encodings and compression
    BinaryWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(T record) {
         Logger log = LoggerFactory.getLogger("binary wrte support");
         log.info("the record {}", record);
         log.info("the cols {}", cols);
        recordConsumer.startMessage();
        //recordConsumer.startField(cols.get(0).getPath()[0], 0);
        //recordConsumer.addBinary((Binary) record);
        recordConsumer.endMessage();
    }
}
