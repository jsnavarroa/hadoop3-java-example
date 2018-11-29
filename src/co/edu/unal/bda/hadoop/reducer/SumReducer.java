package co.edu.unal.bda.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import co.edu.unal.bda.hadoop.TwoTextWritable;

public class SumReducer extends Reducer<TwoTextWritable, IntWritable, TwoTextWritable, IntWritable> {
	private IntWritable result = new IntWritable();

	@Override
	public void reduce(TwoTextWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}