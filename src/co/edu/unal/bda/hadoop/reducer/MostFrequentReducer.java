package co.edu.unal.bda.hadoop.reducer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostFrequentReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		HashMap<Text, Integer> map = new HashMap<>();
		Text mostFrequent = new Text();
		
		for (Text value : values) {
			if(map.containsKey(value)) {
				map.put(value, map.get(value)+1);
			}else {
				map.put(value, 1);
			}
		}
		
		int count = Integer.MIN_VALUE;
		for(Text value: map.keySet()) {
			Integer valueCount = map.get(value);
			if(valueCount > count) {
				mostFrequent.set(value.toString());
				count = valueCount;
			}
		}
		
		context.write(key, mostFrequent);
	}
}