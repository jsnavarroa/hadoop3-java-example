package co.edu.unal.bda.hadoop;

import org.apache.hadoop.io.Text;

import co.edu.unal.bda.hadoop.mapper.LetterWordMapper;
import co.edu.unal.bda.hadoop.reducer.MostFrequentReducer;

public class SingleMapReduceFactory {

	public static SingleMapReduce mostFrequentWordByLetter() {
		return new SingleMapReduce<LetterWordMapper, MostFrequentReducer>(
				"Most frequent word by letter", LetterWordMapper.class, MostFrequentReducer.class,
				Text.class, Text.class);
	}

}