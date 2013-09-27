package by.kslisenko.resourcetree.hostprotocolcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResourceTreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		// TO build directory tree we do not need value data from mapper
//		int total = 0;
//		for (IntWritable value : values) {
//			total++;
//		}
		context.write(text, new IntWritable(0));
	}
}