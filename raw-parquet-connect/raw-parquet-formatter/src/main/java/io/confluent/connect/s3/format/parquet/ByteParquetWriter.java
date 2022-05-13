
package io.confluent.connect.s3.format.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class ByteParquetWriter extends ParquetWriter<MyGroup> {


    public static Builder builder(Path file) {
        return new Builder(file);
    }

    public static Builder builder(OutputFile file) {
        return new Builder(file);
    }


    public ByteParquetWriter(Path file, WriteSupport<MyGroup> writeSupport,
                         CompressionCodecName compressionCodecName,
                         int blockSize, int pageSize, boolean enableDictionary,
                         boolean enableValidation,
                         ParquetProperties.WriterVersion writerVersion,
                         Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    public static class Builder extends ParquetWriter.Builder<MyGroup, Builder> {
        private MessageType type = null;
        private Map<String, String> extraMetaData = new HashMap<String, String>();

        private Builder(Path file) {
            super(file);
        }

        public Builder(OutputFile file) {
            super(file);
        }

        public Builder withType(MessageType type) {
            this.type = type;
            return this;
        }

        public Builder withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData = extraMetaData;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<MyGroup> getWriteSupport(Configuration conf) {
            return new GroupWriteSupport(type, extraMetaData);
        }

    }
}