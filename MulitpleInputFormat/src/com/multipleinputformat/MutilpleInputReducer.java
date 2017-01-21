package com.multipleinputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MutilpleInputReducer extends
		Reducer<LongWritable, Text, LongWritable, IntWritable> {
	String line = null;

	public void reduce(LongWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (Text text : value) {
			count++;
		}

		context.write(key, new IntWritable(count));
	}
}
