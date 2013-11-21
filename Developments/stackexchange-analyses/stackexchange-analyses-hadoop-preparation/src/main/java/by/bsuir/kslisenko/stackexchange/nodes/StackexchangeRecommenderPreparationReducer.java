package by.bsuir.kslisenko.stackexchange.nodes;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StackexchangeRecommenderPreparationReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text text, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(text, value);	
		}
	}
}