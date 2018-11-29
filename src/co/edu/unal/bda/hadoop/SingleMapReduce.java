package co.edu.unal.bda.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.edu.unal.bda.hadoop.mapper.LetterWordMapper;
import co.edu.unal.bda.hadoop.reducer.MostFrequentReducer;

public class SingleMapReduce<M extends Mapper, R extends Reducer> {

	private static final Logger log = LoggerFactory.getLogger(SingleMapReduce.class);
	
	private String jobName;

	private Class<M> mapper;
	private Class<R> reducer;

	private Class<?> outputKeyClass;
	private Class<?> outputKeyValue;
	
	/**
	 * @param jobName
	 * @param mapper
	 * @param reducer
	 * @param outputKeyClass
	 * @param outputKeyValue
	 */
	public SingleMapReduce(String jobName, Class<M> mapper,
			Class<R> reducer, Class outputKeyClass, Class outputKeyValue) {
		super();
		this.jobName = jobName;
		this.mapper = mapper;
		this.reducer = reducer;
		this.outputKeyClass = outputKeyClass;
		this.outputKeyValue = outputKeyValue;
	}
	

	public void run(String input, String output) throws Exception {
		Configuration conf = Main.getConfiguration();

		Job job = Job.getInstance(conf, this.jobName);
		job.setJarByClass(SingleMapReduce.class);
		
		job.setMapperClass(this.mapper);
		job.setReducerClass(this.reducer);
		
		job.setOutputKeyClass(this.outputKeyClass);
		job.setOutputValueClass(this.outputKeyValue);

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