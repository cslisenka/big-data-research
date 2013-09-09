package by.kslisenko.wordcount;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogAnalyseMapper extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Split to strings
		String[] rows = value.toString().split("\n");
		
		// For each row get host as key and protocol as value
		// 1 01/23/1998 16:56:12 00:01:26 <telnet> 1754 23 <192.168.1.30> 192.168.0.20 0 -
		
		Pattern p = Pattern.compile("[\\S]* [\\S]* [\\S]* [\\S]* ([\\S]*) [\\S]* [\\S]* ([\\S]*) [\\S]* [\\S]* [\\S]*");
		
		for (String logEntry : rows) {
			Matcher m = p.matcher(logEntry);
			if (m.find()) {
				String protocol = m.group(1);
				String ip = m.group(2);
				
				outKey.set(ip);
				outValue.set(protocol);
				context.write(outKey, outValue);
			}
		}
	}
	
}