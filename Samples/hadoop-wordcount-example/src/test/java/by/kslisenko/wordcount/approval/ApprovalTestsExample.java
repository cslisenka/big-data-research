package by.kslisenko.wordcount.approval;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.approvaltests.hadoop.version1.HadoopApprovals;
import org.approvaltests.hadoop.version1.MapperWrapper;
import org.approvaltests.hadoop.version1.ReducerWrapper;
import org.approvaltests.hadoop.version1.SmartMapper;
import org.approvaltests.hadoop.version1.SmartReducer;
import org.junit.Test;

import by.kslisenko.wordcount.WordCountMapper;
import by.kslisenko.wordcount.WordCountReducer;

public class ApprovalTestsExample {

	@Test
	public void testMapper() throws Exception {
		SmartMapper<Object, Text, Text, IntWritable> smartMapper = 
				new MapperWrapper<Object, Text, Text, IntWritable>(new WordCountMapper(), Object.class, Text.class, Text.class, IntWritable.class);
		
		HadoopApprovals.verifyMapping(smartMapper, 1, "cat dog dat meat rabbit dog cat");
	}
	
	@Test
	public void testReducer() throws Exception {
		SmartReducer<Text, IntWritable, Text, IntWritable> smartReducer = 
				new ReducerWrapper<Text, IntWritable, Text, IntWritable>(new WordCountReducer(), Text.class, IntWritable.class, Text.class, IntWritable.class);
		HadoopApprovals.verifyReducer(smartReducer, "cat", 1, 1, 1, 1);
	}
	
	@Test
	public void testMapReduce() throws Exception {
		SmartMapper<Object, Text, Text, IntWritable> smartMapper = 
				new MapperWrapper<Object, Text, Text, IntWritable>(new WordCountMapper(), Object.class, Text.class, Text.class, IntWritable.class);
		
		SmartReducer<Text, IntWritable, Text, IntWritable> smartReducer = 
				new ReducerWrapper<Text, IntWritable, Text, IntWritable>(new WordCountReducer(), Text.class, IntWritable.class, Text.class, IntWritable.class);
		
		HadoopApprovals.verifyMapReduce(smartMapper, smartReducer, 1, "cat dog dat meat rabbit dog cat");
	}
}