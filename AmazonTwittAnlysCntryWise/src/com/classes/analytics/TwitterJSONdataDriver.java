package com.classes.analytics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwitterJSONdataDriver {
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        if (args.length != 2) {
//            System.err.println("Usage: CombineBooks <in> <out>");
//            System.exit(2);
//        }

        Job job = new Job(conf, "Twitter Data Hanlder");
        job.setJarByClass(TwitterJSONdataDriver.class);
        job.setMapperClass(TwitterJSONDataMapper.class);
        job.setReducerClass(TwitterDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text .class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);        
        job.setInputFormatClass(JsonInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
