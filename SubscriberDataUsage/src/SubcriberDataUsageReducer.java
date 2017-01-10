import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubcriberDataUsageReducer extends
		Reducer<LongWritable, DoubleWritable, Text, Text> {
	private Text subId = new Text();
	private Text usage = new Text();

	@Override
	public void reduce(LongWritable key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		Double dataUsage = 0.0;
		for (DoubleWritable doubleWritable : values) {
			dataUsage = dataUsage + doubleWritable.get();
		}
		subId.set(String.valueOf(key.get()));
		usage.set("usage is :"  +dataUsage.toString());
		context.write(subId, usage);
	}
}
