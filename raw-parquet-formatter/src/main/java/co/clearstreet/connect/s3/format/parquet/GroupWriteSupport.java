package co.clearstreet.connect.s3.format.parquet;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;


import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupWriteSupport extends WriteSupport<MyGroup> {

    private static final Logger log = LoggerFactory.getLogger(GroupWriteSupport.class);
    public static final String PARQUET_EXAMPLE_SCHEMA = "parquet.example.schema";

    public static void setSchema(MessageType schema, Configuration configuration) {
        configuration.set(PARQUET_EXAMPLE_SCHEMA, schema.toString());
    }

    public static MessageType getSchema(Configuration configuration) {
        return parseMessageType(Objects.requireNonNull(configuration.get(PARQUET_EXAMPLE_SCHEMA), PARQUET_EXAMPLE_SCHEMA));
    }

    private MessageType schema;
    private MyGroupWriter groupWriter;
    private Map<String, String> extraMetaData;

    public GroupWriteSupport() {
        this(null, new HashMap<String, String>());
    }

    GroupWriteSupport(MessageType schema) {
        this(schema, new HashMap<String, String>());
    }

    GroupWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        this.schema = schema;
        this.extraMetaData = extraMetaData;
    }

    @Override
    public String getName() {
        return "example";
    }

    @Override
    public org.apache.parquet.hadoop.api.WriteSupport.WriteContext init(Configuration configuration) {
        // if present, prefer the schema passed to the constructor
        if (schema == null) {
            schema = getSchema(configuration);
        }
        return new WriteContext(schema, this.extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = new MyGroupWriter(recordConsumer, schema);
    }

    public void write(MyGroup record) {
        log.info("CALLED WRITE");
        groupWriter.write(record);
    }

}