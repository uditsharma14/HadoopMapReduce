import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubCriberDataUsageMapper extends
	 	Mapper<LongWritable, Text, LongWritable, DoubleWritable> {
	private LongWritable subsCriberId = new LongWritable();
	private DoubleWritable usagevalue=new DoubleWritable();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	  
      String  line=value.toString();
      String subsId=line.substring(0, 11);
      String usage=line.substring(12,line.length());
      subsCriberId.set(Long.parseLong(subsId));
      usagevalue.set(Double.parseDouble(usage));
      context.write(subsCriberId, usagevalue);
	}

}
