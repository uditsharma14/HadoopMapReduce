package com.classes.nlineinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NLineDriver {
	public static void main(String[] args) throws Exception
	   {
	     Configuration conf = new Configuration();
	     
	     // to specify the separator ( here the key and value is separated by "|")
	     conf.set("mapreduce.input.lineinputformat.linespermap", "2");
	     Job job = new Job(conf);
	     job.setJarByClass(NLineDriver.class);
	     job.setMapperClass(NLineMapper.class);
	     job.setReducerClass(NLineReducer.class);
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(IntWritable.class);
	     job.setInputFormatClass(NLineInputFormat.class);
	     job.setOutputFormatClass(TextOutputFormat.class);
	     FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     System.exit(job.waitForCompletion(true) ? 0 : 1);
	   }
}
