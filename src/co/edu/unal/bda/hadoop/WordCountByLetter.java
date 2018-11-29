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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.edu.unal.bda.hadoop.reducer.IntSumReducer;
import co.edu.unal.bda.hadoop.reducer.SumReducer;

public class WordCountByLetter {

	private static final Logger log = LoggerFactory.getLogger(WordCountByLetter.class);

	public static class LetterWordMapper extends Mapper<Object, Text, TwoTextWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			TwoTextWritable reduceKey = new TwoTextWritable();
			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				String word = itr.nextToken();
				for (int i = 0; i < word.length(); i++) {
					String letter = String.valueOf(word.charAt(i)).toLowerCase();
					reduceKey.set(letter, word);
					context.write(reduceKey, one);
				}
			}
		}
	}

	static void main(String input, String output) throws Exception {
		Configuration conf = Main.getConfiguration();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountByLetter.class);
		job.setMapperClass(LetterWordMapper.class);
		job.setReducerClass(SumReducer.class);
		
		job.setOutputKeyClass(TwoTextWritable.class);
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