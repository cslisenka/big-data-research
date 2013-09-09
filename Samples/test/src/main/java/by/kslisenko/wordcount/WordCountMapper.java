package by.kslisenko.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Object> {

	private Text word = new Text();
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Clean from '"().
		String[] csv = cleanWords(value).split(" ");
		for (String str : csv) {
			word.set(str);
			context.write(word, new IntWritable(1));
		}
	}
	
	protected String cleanWords(Text value) {
		// TODO clean from begin and the end
		String result = value.toString().replace("\"", "");
		result = result.replace("'", "");
		result = result.replace("(", "");
		result = result.replace(")", "");
		result = result.replace(".", "");
		result = result.replace(",", "");
		return result;
	}
}