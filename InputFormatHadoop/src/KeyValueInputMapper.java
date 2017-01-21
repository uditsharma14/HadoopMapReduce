import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyValueInputMapper extends Mapper<Text, Text, Text, IntWritable> {
	public static IntWritable count = new IntWritable(1);

	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		if (null != value) {
			String line = value.toString();
			String[] words = line.split(" ");
			for (String word : words) {
				context.write(new Text(word), count);

			}
		}
	}
}
