package co.edu.unal.bda.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.edu.unal.bda.hadoop.mapper.LetterWordMapper;
import co.edu.unal.bda.hadoop.reducer.MostFrequentReducer;

public class MostFrequentWordByLetter{

	private static final Logger log = LoggerFactory.getLogger(MostFrequentWordByLetter.class);

	static void main(String input, String output) throws Exception {
		Configuration conf = Main.getConfiguration();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MostFrequentWordByLetter.class);
		
		job.setMapperClass(LetterWordMapper.class);
		job.setReducerClass(MostFrequentReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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