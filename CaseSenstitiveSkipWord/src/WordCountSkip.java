import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountSkip extends Configured {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration, "WordCountSkip");
		job.setJobName("enhancedwordcount");
		// Job job=new Job();
		job.setJarByClass(WordCountSkip.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordCountDCMapper.class);
		job.setCombinerClass(WordCountDCReducer.class);
		job.setReducerClass(WordCountDCReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(),
						configuration);
				job.getConfiguration().setBoolean("wordcount.skip.patterns",
						true);
			} else if ("-case".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.case.sensitive",
						false);
			}
			{
				other_args.add(args[i]);
			}
		}
		FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
		job.waitForCompletion(true);
		if(job.waitForCompletion(true)){
			System.out.println(job.getCounters().findCounter(NatureofWords.ALL).getValue());
			System.out.println(job.getCounters().findCounter(NatureofWords.STARTS_WITH_DIGIT).getValue());
			System.out.println(job.getCounters().findCounter(NatureofWords.STARTS_WITH_LETTER).getValue());
		}
	}
}
