package co.edu.unal.bda.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.edu.unal.bda.hadoop.reducer.IntSumReducer;

public class LetterWordCount {

	private static final Logger log = LoggerFactory.getLogger(LetterWordCount.class);

	public static class VowelAtBeginningMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text reduceKey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String letter = String.valueOf(itr.nextToken().charAt(0)).toLowerCase();
				if ("aá".indexOf(letter) >= 0) {
					reduceKey.set("a");
				} else if ("eé".indexOf(letter) >= 0) {
					reduceKey.set("e");
				} else if ("ií".indexOf(letter) >= 0) {
					reduceKey.set("i");
				} else if ("oó".indexOf(letter) >= 0) {
					reduceKey.set("o");
				} else if ("uú".indexOf(letter) >= 0) {
					reduceKey.set("u");
				} else {
					reduceKey.set("Other");
				}
				context.write(reduceKey, one);
			}
		}
	}

	static void main(String input, String output) throws Exception {
		Configuration conf = Main.getConfiguration();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(LetterWordCount.class);
		job.setMapperClass(VowelAtBeginningMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Overwrite output
		FileSystem fileSystem = FileSystem.get(conf);
		Path outputPath = new Path(output);
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		fileSystem.close();

		FileInputFormat.addInputPath(job, new Path(input));

		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);

		log.info("Work done");
	}
}