package by.kslisenko.logfiles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().split(":");
		context.write(new Text(parts[0] + ":" + parts[1]), new IntWritable(1));
	}
}