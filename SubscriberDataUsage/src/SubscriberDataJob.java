import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SubscriberDataJob {
@SuppressWarnings("deprecation")
public static void main(String []args)  throws IOException,
ClassNotFoundException, InterruptedException{
	Configuration conf = new Configuration();
	// conf.
	Job job = new Job(conf, "wcDcache");
	// Job job=new Job();
	job.setJarByClass(SubscriberDataJob.class);

	// Mapper
	job.setMapperClass(SubCriberDataUsageMapper.class);
	job.setReducerClass(SubcriberDataUsageReducer.class);
	
	// Reducer
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(DoubleWritable.class);
	
	// Input Text Format
	job.setInputFormatClass(TextInputFormat.class);
	// Output Text Format
	job.setOutputFormatClass(TextOutputFormat.class);
	// Input directory
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.waitForCompletion(true);
}
}
