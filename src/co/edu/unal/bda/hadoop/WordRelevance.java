package co.edu.unal.bda.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.edu.unal.bda.hadoop.mapper.TermFrecuencyMapper;
import co.edu.unal.bda.hadoop.reducer.IdentityReducer;

public class WordRelevance{

	private static final Logger log = LoggerFactory.getLogger(WordRelevance.class);
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	static void main(String input, String output) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9800");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		Job job = Job.getInstance(conf, "word relevance");
		job.setJarByClass(WordRelevance.class);
		job.setMapperClass(TermFrecuencyMapper.class);
		job.setReducerClass(IdentityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

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