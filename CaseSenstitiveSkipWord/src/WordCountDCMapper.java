import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountDCMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private boolean caseSensitive = true;
	private Set<String> patternsToSkip = new HashSet<String>();
	private long numRecords = 0;
	private String inputFile;
	private BufferedReader fis;
	private boolean skipWord = false;

	protected void setup(Context context) throws IOException,
			InterruptedException {

		try {
			caseSensitive = context.getConfiguration().getBoolean(
					"wordcount.case.sensitive", true);
			inputFile = context.getConfiguration().get("map.input.file");
			if (context.getConfiguration().getBoolean(
					"wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = context.getLocalCacheFiles();
				} catch (IOException ioe) {
					System.err
							.println("Caught exception getting cached files: "
									+ ioe.toString());
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}

		} catch (Exception exception) {
			System.out.println("Exception occured while reading cache files"
					+ exception);
		}
	}

	private void parseSkipFile(Path patternsFile) {
		try {
			fis = new BufferedReader(new FileReader(patternsFile.toString()));
			String pattern = null;
			while ((pattern = fis.readLine()) != null) {
				patternsToSkip.add(pattern);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing the cached file '"
					+ patternsFile + "':" + ioe.toString());
		}
	}
	@Override
	public void map(LongWritable Key, Text value, Context context)
	throws IOException, InterruptedException {
		String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
		StringTokenizer tokenizer = new StringTokenizer(line);	
		while(tokenizer.hasMoreTokens()){
			String token = tokenizer.nextToken();
			if (patternsToSkip.contains(token)){
				System.out.println("Word Skipped");
			}
			else{
				word.set(token);
				context.write(word, one);
			}
		
		}
	}
}
