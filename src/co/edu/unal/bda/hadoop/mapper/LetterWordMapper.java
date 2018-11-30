package co.edu.unal.bda.hadoop.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterWordMapper extends Mapper<Object, Text, Text, Text> {


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Text keyOut = new Text();
		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {
			String word = itr.nextToken();
			for (int i = 0; i < word.length(); i++) {
				String letter = String.valueOf(word.charAt(i)).toLowerCase();
				keyOut.set(letter);
				Text valueOut = new Text(word);
				context.write(keyOut, valueOut);
			}
		}
	}
}