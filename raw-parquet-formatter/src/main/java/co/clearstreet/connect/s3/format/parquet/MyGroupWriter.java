package co.clearstreet.connect.s3.format.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyGroupWriter {

    private static final Logger log = LoggerFactory.getLogger(MyGroup.class);
    private final RecordConsumer recordConsumer;
    private final GroupType schema;

    public MyGroupWriter(RecordConsumer recordConsumer, GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    public void write(Group group) {
        log.info("IN GROUP WRITER WRITE");
        recordConsumer.startMessage();
        writeGroup(group, schema);
        recordConsumer.endMessage();
    }

    private void writeGroup(Group group, GroupType type) {
        int fieldCount = type.getFieldCount();
        for (int field = 0; field < fieldCount; ++field) {
            log.info("HERE {}", field);
            int valueCount = group.getFieldRepetitionCount(field);
            log.info("valueCount={}", valueCount);
            if (valueCount > 0) {
                log.info("IN HERE NOW");
                Type fieldType = type.getType(field);
                String fieldName = fieldType.getName();
                log.info("THE FIELD NAME {}", fieldName);
                recordConsumer.startField(fieldName, field);
                for (int index = 0; index < valueCount; ++index) {
                    log.info("THE INDEX {}", index);
                    if (fieldType.isPrimitive()) {
                        log.info("YES ITS PRIMITVE");
                        group.writeValue(field, index, recordConsumer);
                    } else {
                        log.info("NO ITS NOT PRIMITIVE");
                        recordConsumer.startGroup();
                        writeGroup(group.getGroup(field, index), fieldType.asGroupType());
                        recordConsumer.endGroup();
                    }
                }
                log.info("END FIELD {}", fieldName);
                recordConsumer.endField(fieldName, field);
            }
        }
    }
}
