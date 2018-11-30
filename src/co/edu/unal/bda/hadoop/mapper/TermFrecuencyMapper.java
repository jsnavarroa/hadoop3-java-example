package co.edu.unal.bda.hadoop.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TermFrecuencyMapper extends Mapper<Object, Text, Text, FloatWritable> {
	
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int numOfTerms = 0;
		
		while (itr.hasMoreTokens()) {
			String term = itr.nextToken();
			int count = 1;
			numOfTerms++;
			if(map.containsKey(term)) {
				count += map.get(term);
				map.replace(term, count);
			}else {
				map.put(term, count);
			}
		}
		
		Iterator<Entry<String, Integer>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> pair = (Entry<String, Integer>)it.next();
			word.set(pair.getKey());
			float frecuency = (float) pair.getValue() / numOfTerms;
			context.write(word, new FloatWritable(frecuency));
		}
	}
}
