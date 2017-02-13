package com.src;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class CompositeJoinMapper extends
	 	Mapper<Text, TupleWritable, Text, Text> {
	
	@Override
	public void map(Text key, TupleWritable value, Context context)
			throws IOException, InterruptedException {
	  String Mkey=value.get(0).toString();
	  String val=value.get(0).toString()+","+value.get(1).toString();
      context.write(key, new Text(val));
	}
}