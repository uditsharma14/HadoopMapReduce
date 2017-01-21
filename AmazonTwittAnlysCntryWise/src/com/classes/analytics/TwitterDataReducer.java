package com.classes.analytics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwitterDataReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int value = 0;
		while (values.iterator().hasNext()) {
			values.iterator().next();
			++value;
		}
		context.write(key, new IntWritable(value));
		value=0;
	}

}
