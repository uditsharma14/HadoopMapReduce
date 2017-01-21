package com.multipleinputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTwo extends Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lines = value.toString().trim().split("\\|");
		context.write(new LongWritable(Long.parseLong(lines[1])), new Text(
				lines[0]));
	}
}
