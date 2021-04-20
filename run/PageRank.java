import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Page rank in Hadoop.
 * 
 * References: 
 * - Get text between strings https://stackoverflow.com/a/16597374
 * 
 * @author John Chittam
 */
public class PageRank {

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String valueStr = value.toString();
			
			String title = null;
			Pattern titlePattern = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
			Matcher titleMatcher = titlePattern.matcher(valueStr);
			
			while (titleMatcher.find()) {
				title = titleMatcher.group(1);
			}
			
			Pattern linkPattern = Pattern.compile("\\[\\[(.*?)]]", Pattern.DOTALL);
			Matcher linkMatcher = linkPattern.matcher(valueStr);
			
			if (!linkMatcher.find()) {
				context.write(new Text(title), new Text(""));
			} else {
				linkMatcher.reset();
				while (linkMatcher.find()) {
					context.write(new Text(title), new Text(linkMatcher.group(1)));
				}
			}
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, ArrayListWritable<Text>> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayListWritable<Text> result = new ArrayListWritable<Text>();
			for (Text val : values) {
				String formattedVal = this.formatLink(val.toString());
				if (!formattedVal.isEmpty()) {
					result.add(new Text(formattedVal));
				}
			}
			context.write(key, result);
		}
		
		private String formatLink(String link) {
			if (link.trim().isEmpty() || link.contains(":")) {
				return "";
			}
			if (link.contains("\\|")) {
				return link.split("\\|")[0];
			}
			
			return link;
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "page rank");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapper.class);
		job.setCombinerClass(PageRankReducer.class);
		job.setReducerClass(PageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayListWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
