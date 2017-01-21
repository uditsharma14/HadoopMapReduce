package com.classes.nlineinput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public static IntWritable count = new IntWritable(1);

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (null != value) {
			String line = value.toString();
			String[] words = line.split(" ");
			for (String word : words) {
				context.write(new Text(word), count);

			}
		}
	}
}
