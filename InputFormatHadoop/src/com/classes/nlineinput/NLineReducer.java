package com.classes.nlineinput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int wordCount = 0;
		for (IntWritable value : values) {
			wordCount++;
		}
		context.write(key, new IntWritable(wordCount));

	}
}
