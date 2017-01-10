import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/** Auther Udit Sharma **/
public class WordCount {

	public static class WordMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		BufferedReader fis = null;
		private Set<String> stopWordset = new HashSet<String>();

		public void map(LongWritable Key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer stringTokenizer = new StringTokenizer(line);
			while (stringTokenizer.hasMoreTokens()) {
				word.set(stringTokenizer.nextToken());
				if (!stopWordset.contains(word.toString())) {
					context.write(word, one);
				}
			}
		}

		private void readStopWordFile(Path stopPathFile) {
			try {
				fis = new BufferedReader(
						new FileReader(stopPathFile.toString()));
				String stopWord = null;
				while ((stopWord = fis.readLine()) != null) {
					stopWordset.add(stopWord.trim());
				}
			} catch (IOException exception) {
				System.out.println("Exception " + exception);
			}
		}

		protected void setup(Context context) throws IOException,
				InterruptedException {

			try {
				Path[] paths = context.getLocalCacheFiles();
				if (paths != null && paths.length > 0) {
					readStopWordFile(paths[0]);
				}

			} catch (Exception exception) {
				System.out
						.println("Exception occured while reading cache files"
								+ exception);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable intWritable : values) {
				sum += intWritable.get();

			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// conf.
		Job job = new Job(conf, "wcDcache");
		// Job job=new Job();
		job.setJarByClass(WordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// Mapper
		job.setMapperClass(WordMapper.class);
		job.setCombinerClass(Reduce.class);
		// Reducer
		job.setReducerClass(Reduce.class);
		
		
		//DistributedCache.addCacheFile(uri, conf)
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
       // DistributedCache.addCacheFile();
		// Input Text Format
		job.setInputFormatClass(TextInputFormat.class);
		// Output Text Format
		job.setOutputFormatClass(TextOutputFormat.class);
		// Input directory
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Output directory
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		


		job.waitForCompletion(true);
	}
}
