
package co.clearstreet.connect.s3.format.parquet;

import co.clearstreet.connect.s3.S3SinkConnectorConfig;
import co.clearstreet.connect.s3.format.RecordViewSetter;
import co.clearstreet.connect.s3.format.S3RetriableRecordWriter;
import co.clearstreet.connect.s3.storage.IORecordWriter;
import co.clearstreet.connect.s3.storage.S3ParquetOutputStream;
import co.clearstreet.connect.s3.storage.S3Storage;
import co.clearstreet.connect.s3.util.Utils;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

import org.apache.kafka.connect.converters.ByteArrayConverter;

import org.apache.kafka.connect.sink.SinkRecord;

import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.GroupType;

import java.io.IOException;


public class ParquetRecordWriterProvider<T> extends RecordViewSetter
    implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private static final int PAGE_SIZE = 64 * 1024;
  private final S3Storage storage;
  private final ByteArrayConverter converter;

  ParquetRecordWriterProvider(S3Storage storage, ByteArrayConverter converter) {
    this.storage = storage;
    this.converter = converter;
  }

  public MessageType getSchemaForParquetFile() throws IOException {
    String rawSchema = "message blob { required BINARY raw_bytes;}";
    return MessageTypeParser.parseMessageType(rawSchema);
  }
  @Override
  public String getExtension() {
    return storage.conf().parquetCompressionCodecName().getExtension() + EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    return new S3RetriableRecordWriter(
        new IORecordWriter() {
          final String adjustedFilename = Utils.getAdjustedFilename(recordView, filename, getExtension());


          final MessageType schema;

          {
            try {
              schema = getSchemaForParquetFile();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          final S3ParquetOutputFile s3ParquetOutputFile = new S3ParquetOutputFile(storage, adjustedFilename);
          final ParquetWriter<MyGroup> writer;
            {
                try {
                    writer = new ByteParquetWriter.Builder(s3ParquetOutputFile).withType(schema).build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }



            @Override
          public void write(SinkRecord record) throws IOException {
            byte[] bytes = converter.fromConnectData(record.topic(),
                    recordView.getViewSchema(record, false), recordView.getView(record, false));
            log.info("the bytes: {}", bytes);
            log.info("the value: {}", record.value());
            log.info("the string {}", record.value().toString());
            log.info("we do make it here");
            log.info("my writer {}", writer);
            GroupType groupType = new GroupType(Type.Repetition.REQUIRED, "raw_bytes", schema);
            MyGroup group = new MyGroup(groupType);
            //SimpleGroup group = new SimpleGroup(groupType);
            group.add(0, record.value());
            writer.write(group);
          }

          @Override
          public void close() throws IOException{
              writer.close();
          }

          @Override
          public void commit() throws IOException {
            s3ParquetOutputFile.s3out.setCommit();
              writer.close();
          }
        }
    );
  }

  private static class S3ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;
    private S3Storage storage;
    private String filename;
    private S3ParquetOutputStream s3out;

    S3ParquetOutputFile(S3Storage storage, String filename) {
      this.storage = storage;
      this.filename = filename;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      s3out = (S3ParquetOutputStream) storage.create(filename, true, ParquetFormat.class);
      return s3out;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return DEFAULT_BLOCK_SIZE;
    }
  }
}
