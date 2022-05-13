package io.confluent.connect.s3.format.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.BinaryValue;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.Primitive;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MyGroup extends Group {
    private static final Logger log = LoggerFactory.getLogger(MyGroup.class);

    private final GroupType schema;
    private final List<Object>[] data;

    @SuppressWarnings("unchecked")
    public MyGroup(GroupType schema) {
        this.schema = schema;
        this.data = new List[schema.getFields().size()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            this.data[i] = new ArrayList<Object>();
        }
    }

    public void add(int fieldIndex, Object value) {
        List<Object> list = data[fieldIndex];
        list.add(value);
    }

    @Override
    public void add(int fieldIndex, int value) {

    }

    @Override
    public void add(int fieldIndex, long value) {

    }

    @Override
    public void add(int fieldIndex, String value) {

    }

    @Override
    public void add(int fieldIndex, boolean value) {

    }

    @Override
    public void add(int fieldIndex, NanoTime value) {

    }

    @Override
    public void add(int fieldIndex, Binary value) {

    }

    @Override
    public void add(int fieldIndex, float value) {

    }

    @Override
    public void add(int fieldIndex, double value) {

    }

    @Override
    public void add(int fieldIndex, Group value) {

    }

    private Object getValue(int fieldIndex, int index) {
        log.info("IN GET VALUE");
        List<Object> list;
        try {
            list = data[fieldIndex];
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") in group:\n" + this);
        }
        try {
            return list.get(index);
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("not found " + fieldIndex + "(" + schema.getFieldName(fieldIndex) + ") element number " + index + " in group:\n" + this);
        }
    }
    @Override
    public Group addGroup(int fieldIndex) {
        return null;
    }

    @Override
    public int getFieldRepetitionCount(int fieldIndex) {
        List<Object> list = data[fieldIndex];
        return list == null ? 0 : list.size();
    }

    @Override
    public Group getGroup(int fieldIndex, int index) {
        return (Group)getValue(fieldIndex, index);
    }

    @Override
    public String getString(int fieldIndex, int index) {
        return null;
    }

    @Override
    public int getInteger(int fieldIndex, int index) {
        return 0;
    }

    @Override
    public long getLong(int fieldIndex, int index) {
        return 0;
    }

    @Override
    public double getDouble(int fieldIndex, int index) {
        return 0;
    }

    @Override
    public float getFloat(int fieldIndex, int index) {
        return 0;
    }

    @Override
    public boolean getBoolean(int fieldIndex, int index) {
        return false;
    }

    @Override
    public Binary getBinary(int fieldIndex, int index) {
        log.info("GET BINARY");
        return ((BinaryValue)getValue(fieldIndex, index)).getBinary();
    }

    @Override
    public Binary getInt96(int fieldIndex, int index) {
        return null;
    }

    @Override
    public String getValueToString(int fieldIndex, int index) {
        return null;
    }

    @Override
    public GroupType getType() {
        return schema;
    }

    @Override
    public void writeValue(int field, int index, RecordConsumer recordConsumer) {
        log.info("IN WRITE VALUE");

        Binary val = getBinary(field, index);
        recordConsumer.addBinary(val);
        log.info("from the write value {}", val);
    //    val.writeValue(recordConsumer);
    }
}