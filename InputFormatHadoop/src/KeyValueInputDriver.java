import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KeyValueInputDriver {
	public static void main(String[] args) throws Exception
	   {
	     Configuration conf = new Configuration();
	     
	     // to specify the separator ( here the key and value is separated by "|")
	     conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");

	     Job job = new Job(conf);
	     job.setJarByClass(KeyValueInputDriver.class);
	     job.setMapperClass(KeyValueInputMapper.class);
	     job.setReducerClass(KeyValueInputReducer.class);
	     job.setMapOutputKeyClass(Text.class);
	     
	     job.setMapOutputValueClass(IntWritable.class);
	     
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(IntWritable.class);
	     job.setInputFormatClass(KeyValueTextInputFormat.class);
	     job.setOutputFormatClass(TextOutputFormat.class);
	     FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     System.exit(job.waitForCompletion(true) ? 0 : 1);
	   }
}
