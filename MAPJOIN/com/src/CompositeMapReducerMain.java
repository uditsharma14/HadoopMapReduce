package com.src;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompositeMapReducerMain {
	public static void main(String []args)  throws IOException,
	ClassNotFoundException, InterruptedException{
		Path left= new Path(args[0]);
		Path rigth = new Path(args[1]);
		Configuration config = new Configuration();
		String joinExpression = CompositeInputFormat.compose("inner",   KeyValueTextInputFormat.class, left, rigth);
		config.set("mapreduce.join.expr", joinExpression);
		Job job =  Job.getInstance(config);
		job.setJobName("Blah");
		job.setJarByClass(CompositeMapReducerMain.class);

		// Mapper
		job.setMapperClass(CompositeJoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setInputFormatClass(CompositeInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.waitForCompletion(true);
	}
}
