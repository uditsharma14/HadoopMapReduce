package com.classes.analytics;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import com.google.gson.JsonObject;
import org.slf4j.*;

import java.io.IOException;
import java.util.Map.Entry;

/**
 * Assumes one line per JSON object
 */
public class JsonInputFormat
    extends FileInputFormat<LongWritable, MapWritable> {

  private static final Logger log =
      LoggerFactory.getLogger(JsonInputFormat.class);

  @Override
  public RecordReader<LongWritable, MapWritable> createRecordReader(
      InputSplit split,
      TaskAttemptContext
          context) {
    return new JsonRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec =
        new CompressionCodecFactory(context.getConfiguration())
            .getCodec(file);
    return codec == null;
  }

  public static class JsonRecordReader
      extends RecordReader<LongWritable, MapWritable> {
    private static final Logger LOG =
        LoggerFactory.getLogger(JsonRecordReader
            .class);

    private LineRecordReader reader = new LineRecordReader();
    private final Text currentLine_ = new Text();
    private final MapWritable value_ = new MapWritable();
    private final JsonParser jsonParser_ = new JsonParser();

    @Override
    public void initialize(InputSplit split,
                           TaskAttemptContext context)
        throws IOException, InterruptedException {
      reader.initialize(split, context);
    }

    @Override
    public synchronized void close() throws IOException {
      reader.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
        InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public MapWritable getCurrentValue() throws IOException,
        InterruptedException {
      return value_;
    }

    @Override
    public float getProgress()
        throws IOException, InterruptedException {
      return reader.getProgress();
    }

    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException {
      while (reader.nextKeyValue()) {
        value_.clear();
        if (decodeLineToJson(jsonParser_, reader.getCurrentValue(),
            value_)) {
          return true;
        }
      }
      return false;
    }

    public static boolean decodeLineToJson(JsonParser parser,
                                           Text line,
                                           MapWritable value) {
      //log.info("Got string '{}'", line);
      try {
        JsonObject jsonObj =
            (JsonObject) parser.parse(line.toString());
        for (Entry<String, JsonElement> key : jsonObj.entrySet()) {
          Text mapKey = new Text(key.getKey());
          Text mapValue = new Text();
          if (key.getValue() != null) {
            mapValue.set(key.getValue().toString());
          }

          value.put(mapKey, mapValue);
        }
        return true;
      } catch (JsonSyntaxException e) {
        LOG.warn("Could not json-decode string: " + line, e);
        return false;
      } catch (NumberFormatException e) {
        LOG.warn("Could not parse field into number: " + line, e);
        return false;
      }
    }
  }
}
