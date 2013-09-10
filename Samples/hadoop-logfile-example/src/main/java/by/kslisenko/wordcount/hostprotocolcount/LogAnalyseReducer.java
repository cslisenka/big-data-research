package by.kslisenko.wordcount.hostprotocolcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogAnalyseReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text text, Iterable<Text> values, Context cont) 
			throws IOException, InterruptedException {
		Map<String, Integer> protocolsCount = new HashMap<String, Integer>(); 
		// Collect number of protocols per host
		for (Text value : values) {
			if (protocolsCount.get(value.toString()) == null) {
				protocolsCount.put(value.toString(), 1);
			} else {
				protocolsCount.put(value.toString(), protocolsCount.get(value.toString()) + 1);
			}
		}
		
		// Write hash map to output
		for (String protocol : protocolsCount.keySet()) {
			cont.write(new Text(text.toString() + " " + protocol), new IntWritable(protocolsCount.get(protocol)));	
		}
	}
}