package by.kslisenko.resourcetree.nodes;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import by.kslisenko.resourcetree.utils.FilePathUtil;

public class ResourceTreeNodesMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Parsing log line
		// For each row get host as key and protocol as value
        // 1               2 3 4                     5      6    7                          8         9   10
		// uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0
		Pattern p = Pattern.compile("([\\S]*) [\\S]* [\\S]* [\\S]* [\\S]* [\\S]* ([\\S]*) [\\S]* [\\S]*");
		Matcher m = p.matcher(value.toString());
		if (m.find()) {
			processHost(m.group(1), context);
			processFile(m.group(2), context);
		}
	}
	
	protected void processFile(String filename, Context context) throws IOException, InterruptedException {
		// 1. Write file node
		Map<String, String> parentChildFiles = FilePathUtil.getParentChildPairs(filename);
		for (String singleFile : parentChildFiles.values()) {
			context.write(new Text("file " + singleFile), new IntWritable(1));	
		}
	}
	
	protected void processHost(String hostname, Context context) throws IOException, InterruptedException {
		context.write(new Text("host " + hostname), new IntWritable(1));
	}
}