package com.multipleinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutipleInputDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(MutipleInputDriver.class);
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, MapperOne.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, MapperTwo.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setReducerClass(MutilpleInputReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
	     System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
