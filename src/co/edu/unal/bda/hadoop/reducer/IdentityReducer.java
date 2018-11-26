package co.edu.unal.bda.hadoop.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IdentityReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

}
