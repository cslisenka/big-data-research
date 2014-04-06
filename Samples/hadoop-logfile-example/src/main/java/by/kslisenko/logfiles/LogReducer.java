package by.kslisenko.logfiles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> values, Context cont) 
			throws IOException, InterruptedException {
		int counter = 0;
		for (IntWritable value : values) {
			counter++;
		}
		cont.write(text, new IntWritable(counter));	
	}
}