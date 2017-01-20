package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;

public class DBIODriver {
	 public static void main(String[] args) throws Exception
	   {
	     Configuration conf = new Configuration();
	     DBConfiguration.configureDB(conf,
	     "com.mysql.jdbc.Driver",   // driver class
	     "jdbc:mysql://localhost:3306/hadoopdb", // db url
	     "root",    // user name
	     "udit"); //password

	     Job job = new Job(conf);
	     job.setJarByClass(DBIODriver.class);
	     job.setMapperClass(Map.class);
	     job.setReducerClass(Reduce.class);
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(IntWritable.class);
	     job.setOutputKeyClass(DBOutputWritable.class);
	     job.setOutputValueClass(NullWritable.class);
	     job.setInputFormatClass(DBInputFormat.class);
	     job.setOutputFormatClass(DBOutputFormat.class);

	     DBInputFormat.setInput(
	     job,
	     DBInputWritable.class,
	     "Employee",   //input table name
	     null,
	     null,
	     new String[] { "empid", "name" }  // table columns
	     );

	     DBOutputFormat.setOutput(
	     job,
	     "output",    // output table name
	     new String[] { "name", "count" }   //table columns
	     );

	     System.exit(job.waitForCompletion(true) ? 0 : 1);
	   }
}
