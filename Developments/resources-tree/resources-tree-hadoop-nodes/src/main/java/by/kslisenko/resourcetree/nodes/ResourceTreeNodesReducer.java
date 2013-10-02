package by.kslisenko.resourcetree.nodes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResourceTreeNodesReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		// Write header for csv
		context.write(new Text("name:string:files"), new Text("type"));
	}

	@Override
	protected void reduce(Text text, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

		String[] parts = text.toString().split(" ");
		if (parts.length == 2) {
			// parts[0] = host/file
			// parts[1] = node name
			// output: [file name]	file
			// or: [host name]	host
			context.write(new Text(parts[1]), new Text(parts[0]));	
		}
	}
}