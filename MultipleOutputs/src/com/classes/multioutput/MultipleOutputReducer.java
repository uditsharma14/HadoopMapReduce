package com.classes.multioutput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultipleOutputReducer extends
		Reducer<LongWritable, Text, LongWritable, IntWritable> {
	String line = null;
	private MultipleOutputs<LongWritable, IntWritable> multipleOutputs;

	public void reduce(LongWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (Text text : value) {
			count++;
		}
		multipleOutputs.write(key, new IntWritable(count), key.toString());
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs = new MultipleOutputs<LongWritable, IntWritable>(
				context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();

	}
}
