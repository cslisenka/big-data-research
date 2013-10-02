package by.kslisenko.resourcetree.nodes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResourceTreeRelationshipsReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		context.write(new Text("name:string:files	name:string:files"), new Text("type"));
	}

	@Override
	protected void reduce(Text text, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		String[] parts = text.toString().split("	");
		if (parts.length == 3) {
			// parts[0] = [parent file]
			// parts[1] = [child file]
			// parts[2] = CONTAINS
			context.write(new Text(parts[0] + "	" + parts[1]), new Text(parts[2]));	
		}
	}
}