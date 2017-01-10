import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountDCReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		String token = key.toString();
		context.getCounter(NatureofWords.ALL).increment(1);
		if (StringUtils.startsWithDigit(token)) {
			context.getCounter(NatureofWords.STARTS_WITH_DIGIT).increment(1);
		} else if (StringUtils.startsWithLetter(token)) {
			context.getCounter(NatureofWords.STARTS_WITH_LETTER).increment(1);
		}
		context.getCounter(NatureofWords.ALL).increment(1);
		while (values.hasNext()) {
			sum += values.next().get();
		}
		context.write(key, new IntWritable(sum));
	}
}
